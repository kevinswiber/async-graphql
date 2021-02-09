use std::any::Any;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use async_graphql_value::ConstValue;
use futures_util::stream::{Stream, StreamExt};
use indexmap::map::IndexMap;

use super::query_root::QueryRoot;
use crate::context::{Data, QueryEnvInner, ResolveId};
use crate::extensions::{ErrorLogger, ExtensionContext, ExtensionFactory, Extensions};
use crate::model::__DirectiveLocation;
use crate::parser::parse_query;
use crate::parser::types::{DocumentOperations, OperationType};
use crate::registry::{MetaDirective, MetaInputValue, MetaType, Registry};
use crate::schema::SchemaEnvInner;
use crate::validation::{check_rules, ValidationMode};
use crate::{
    BatchRequest, BatchResponse, CacheControl, ContextBase, QueryEnv, Request, Response, SchemaEnv,
    ServerError, Type, ID,
};

/// Schema builder
pub struct SchemaBuilder {
    validation_mode: ValidationMode,
    query: QueryRoot,
    mutation: MetaType,
    subscription: MetaType,
    registry: Registry,
    data: Data,
    complexity: Option<usize>,
    depth: Option<usize>,
    extensions: Vec<Box<dyn ExtensionFactory>>,
    enable_federation: bool,
}

impl SchemaBuilder {
    /// Manually register a raw GraphQL type in the schema.
    ///
    /// You can use this to register schema types that don't have an associated Rust type.
    pub fn update_registry<F: FnMut(&mut Registry)>(mut self, f: &mut F) -> Self {
        f(&mut self.registry);
        self
    }

    /// Disable introspection queries.
    pub fn disable_introspection(mut self) -> Self {
        self.query.disable_introspection = true;
        self
    }

    /// Set the maximum complexity a query can have. By default, there is no limit.
    pub fn limit_complexity(mut self, complexity: usize) -> Self {
        self.complexity = Some(complexity);
        self
    }

    /// Set the maximum depth a query can have. By default, there is no limit.
    pub fn limit_depth(mut self, depth: usize) -> Self {
        self.depth = Some(depth);
        self
    }

    /// Add an extension to the schema.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use async_graphql::*;
    ///
    /// struct Query;
    ///
    /// #[Object]
    /// impl Query {
    ///     async fn value(&self) -> i32 {
    ///         100
    ///     }
    /// }
    ///
    /// let schema = Schema::build(Query, EmptyMutation,EmptySubscription)
    ///     .extension(extensions::Logger)
    ///     .finish();
    /// ```
    pub fn extension(mut self, extension: impl ExtensionFactory) -> Self {
        self.extensions.push(Box::new(extension));
        self
    }

    /// Add a global data that can be accessed in the `Schema`. You access it with `Context::data`.
    pub fn data<D: Any + Send + Sync>(mut self, data: D) -> Self {
        self.data.insert(data);
        self
    }

    /// Set the validation mode, default is `ValidationMode::Strict`.
    pub fn validation_mode(mut self, validation_mode: ValidationMode) -> Self {
        self.validation_mode = validation_mode;
        self
    }

    /// Enable federation, which is automatically enabled if the Query has least one entity definition.
    pub fn enable_federation(mut self) -> Self {
        self.enable_federation = true;
        self
    }

    /// Override the description of the specified type.
    pub fn override_description<T: Type>(mut self, desc: &'static str) -> Self {
        self.registry.set_description::<T>(desc);
        self
    }

    /// Build schema.
    pub fn finish(mut self) -> Schema {
        // federation
        if self.enable_federation || self.registry.has_entities() {
            self.registry.create_federation_types();
        }

        Schema(Arc::new(SchemaInner {
            validation_mode: self.validation_mode,
            query: self.query,
            mutation: self.mutation,
            subscription: self.subscription,
            complexity: self.complexity,
            depth: self.depth,
            extensions: self.extensions,
            env: SchemaEnv(Arc::new(SchemaEnvInner {
                registry: self.registry,
                data: self.data,
            })),
        }))
    }
}

#[doc(hidden)]
pub struct SchemaInner {
    pub(crate) validation_mode: ValidationMode,
    pub(crate) query: QueryRoot,
    pub(crate) mutation: MetaType,
    pub(crate) subscription: MetaType,
    pub(crate) complexity: Option<usize>,
    pub(crate) depth: Option<usize>,
    pub(crate) extensions: Vec<Box<dyn ExtensionFactory>>,
    pub(crate) env: SchemaEnv,
}

/// GraphQL schema.
///
/// Cloning a schema is cheap, so it can be easily shared.
pub struct Schema(Arc<SchemaInner>);

impl Clone for Schema {
    fn clone(&self) -> Self {
        Schema(self.0.clone())
    }
}

impl Deref for Schema {
    type Target = SchemaInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Schema {
    /// Create a schema builder
    ///
    /// The root object for the query and Mutation needs to be specified.
    /// If there is no mutation, you can use `EmptyMutation`.
    /// If there is no subscription, you can use `EmptySubscription`.
    pub fn build(query: MetaType, mutation: MetaType, subscription: MetaType) -> SchemaBuilder {
        let registry = Self::create_registry(&query, &mutation, &subscription);
        SchemaBuilder {
            validation_mode: ValidationMode::Strict,
            query: QueryRoot::new(query),
            mutation,
            subscription,
            registry,
            data: Default::default(),
            complexity: None,
            depth: None,
            extensions: Default::default(),
            enable_federation: false,
        }
    }

    /// Create a schema builder
    ///
    /// The root object for the query and Mutation needs to be specified.
    /// If there is no mutation, you can use `EmptyMutation`.
    /// If there is no subscription, you can use `EmptySubscription`.
    pub fn build_with_registry(
        query: MetaType,
        mutation: MetaType,
        subscription: MetaType,
        registry: Registry,
    ) -> SchemaBuilder {
        SchemaBuilder {
            validation_mode: ValidationMode::Strict,
            query: QueryRoot::new(query),
            mutation,
            subscription,
            registry,
            data: Default::default(),
            complexity: None,
            depth: None,
            extensions: Default::default(),
            enable_federation: false,
        }
    }

    fn get_name_from_meta_type(e: &MetaType) -> &str {
        match e {
            MetaType::Object { name, .. } => name,
            MetaType::Enum { name, .. } => name,
            MetaType::Interface { name, .. } => name,
            MetaType::InputObject { name, .. } => name,
            MetaType::Scalar { name, .. } => name,
            MetaType::Union { name, .. } => name,
        }
    }

    pub(crate) fn create_registry(
        query: &MetaType,
        mutation: &MetaType,
        subscription: &MetaType,
    ) -> Registry {
        let mut registry = Registry {
            types: Default::default(),
            directives: Default::default(),
            implements: Default::default(),
            query_type: Self::get_name_from_meta_type(query).to_string(),
            mutation_type: Some(Self::get_name_from_meta_type(mutation).to_string()),
            subscription_type: Some(Self::get_name_from_meta_type(subscription).to_string()),
        };

        registry.add_directive(MetaDirective {
            name: "include",
            description: Some("Directs the executor to include this field or fragment only when the `if` argument is true."),
            locations: vec![
                __DirectiveLocation::FIELD,
                __DirectiveLocation::FRAGMENT_SPREAD,
                __DirectiveLocation::INLINE_FRAGMENT
            ],
            args: {
                let mut args = IndexMap::new();
                args.insert("if", MetaInputValue {
                    name: "if",
                    description: Some("Included when true."),
                    ty: "Boolean!".to_string(),
                    default_value: None,
                    validator: None,
                    visible: None,
                });
                args
            }
        });

        registry.add_directive(MetaDirective {
            name: "skip",
            description: Some("Directs the executor to skip this field or fragment when the `if` argument is true."),
            locations: vec![
                __DirectiveLocation::FIELD,
                __DirectiveLocation::FRAGMENT_SPREAD,
                __DirectiveLocation::INLINE_FRAGMENT
            ],
            args: {
                let mut args = IndexMap::new();
                args.insert("if", MetaInputValue {
                    name: "if",
                    description: Some("Skipped when true."),
                    ty: "Boolean!".to_string(),
                    default_value: None,
                    validator: None,
                    visible: None,
                });
                args
            }
        });

        registry.add_directive(MetaDirective {
            name: "ifdef",
            description: Some("Directs the executor to query only when the field exists."),
            locations: vec![__DirectiveLocation::FIELD],
            args: Default::default(),
        });

        // register scalars
        bool::create_type_info(&mut registry);
        i32::create_type_info(&mut registry);
        f32::create_type_info(&mut registry);
        String::create_type_info(&mut registry);
        ID::create_type_info(&mut registry);

        registry
    }

    /// Create a schema
    pub fn new(query: MetaType, mutation: MetaType, subscription: MetaType) -> Schema {
        Schema::build(query, mutation, subscription).finish()
    }

    /// Returns SDL(Schema Definition Language) of this schema.
    pub fn sdl(&self) -> String {
        self.0.env.registry.export_sdl(false)
    }

    /// Returns Federation SDL(Schema Definition Language) of this schema.
    pub fn federation_sdl(&self) -> String {
        self.0.env.registry.export_sdl(true)
    }

    /// Get all names in this schema
    ///
    /// Maybe you want to serialize a custom binary protocol. In order to minimize message size, a dictionary
    /// is usually used to compress type names, field names, directive names, and parameter names. This function gets all the names,
    /// so you can create this dictionary.
    pub fn names(&self) -> Vec<String> {
        self.0.env.registry.names()
    }

    async fn prepare_request(
        &self,
        request: Request,
    ) -> Result<(QueryEnvInner, CacheControl), Vec<ServerError>> {
        // create extension instances
        let mut extensions: Extensions = self
            .0
            .extensions
            .iter()
            .map(|factory| factory.create())
            .collect::<Vec<_>>()
            .into();

        let mut request = request;
        let data = std::mem::take(&mut request.data);
        let ctx_extension = ExtensionContext {
            schema_data: &self.env.data,
            query_data: &data,
        };

        let request = extensions.prepare_request(&ctx_extension, request).await?;

        extensions.parse_start(&ctx_extension, &request.query, &request.variables);
        let document = parse_query(&request.query)
            .map_err(Into::<ServerError>::into)
            .log_error(&ctx_extension, &extensions)?;
        extensions.parse_end(&ctx_extension, &document);

        // check rules
        extensions.validation_start(&ctx_extension);
        let validation_result = check_rules(
            &self.env.registry,
            &document,
            Some(&request.variables),
            self.validation_mode,
        )
        .log_error(&ctx_extension, &extensions)?;
        extensions.validation_end(&ctx_extension, &validation_result);

        // check limit
        if let Some(limit_complexity) = self.complexity {
            if validation_result.complexity > limit_complexity {
                return Err(vec![ServerError::new("Query is too complex.")])
                    .log_error(&ctx_extension, &extensions);
            }
        }

        if let Some(limit_depth) = self.depth {
            if validation_result.depth > limit_depth {
                return Err(vec![ServerError::new("Query is nested too deep.")])
                    .log_error(&ctx_extension, &extensions);
            }
        }

        let operation = if let Some(operation_name) = &request.operation_name {
            match document.operations {
                DocumentOperations::Single(_) => None,
                DocumentOperations::Multiple(mut operations) => {
                    operations.remove(operation_name.as_str())
                }
            }
            .ok_or_else(|| {
                ServerError::new(format!(r#"Unknown operation named "{}""#, operation_name))
            })
        } else {
            match document.operations {
                DocumentOperations::Single(operation) => Ok(operation),
                DocumentOperations::Multiple(map) if map.len() == 1 => {
                    Ok(map.into_iter().next().unwrap().1)
                }
                DocumentOperations::Multiple(_) => {
                    Err(ServerError::new("Operation name required in request."))
                }
            }
        };
        let operation = match operation {
            Ok(operation) => operation,
            Err(e) => {
                extensions.error(&ctx_extension, &e);
                return Err(vec![e]);
            }
        };

        let env = QueryEnvInner {
            extensions,
            variables: request.variables,
            operation,
            fragments: document.fragments,
            uploads: request.uploads,
            ctx_data: Arc::new(data),
            http_headers: Default::default(),
        };
        Ok((env, validation_result.cache_control))
    }

    async fn execute_once(&self, env: QueryEnv) -> Response {
        // execute
        let inc_resolve_id = AtomicUsize::default();
        let ctx = ContextBase {
            path_node: None,
            resolve_id: ResolveId::root(),
            inc_resolve_id: &inc_resolve_id,
            item: &env.operation.node.selection_set,
            schema_env: &self.env,
            query_env: &env,
        };
        let ctx_extension = ExtensionContext {
            schema_data: &self.env.data,
            query_data: &env.ctx_data,
        };

        env.extensions.execution_start(&ctx_extension);
        // TODO: Fetch real response.
        let data: Result<ConstValue, ServerError> = match &env.operation.node.ty {
            OperationType::Query => Ok(ConstValue::Null), //resolve_container(&ctx, &self.query).await,
            OperationType::Mutation => Ok(ConstValue::Null), //resolve_container_serial(&ctx, &self.mutation).await,
            OperationType::Subscription => {
                return Response::from_errors(vec![ServerError::new(
                    "Subscriptions are not supported on this transport.",
                )])
            }
        };

        env.extensions.execution_end(&ctx_extension);
        let extensions = env.extensions.result(&ctx_extension);

        match data {
            Ok(data) => Response::new(data),
            Err(e) => Response::from_errors(vec![e]),
        }
        .extensions(extensions)
        .http_headers(std::mem::take(&mut *env.http_headers.lock()))
    }

    /// Execute a GraphQL query.
    pub async fn execute(&self, request: impl Into<Request>) -> Response {
        let request = request.into();
        match self.prepare_request(request).await {
            Ok((env, cache_control)) => self
                .execute_once(QueryEnv::new(env))
                .await
                .cache_control(cache_control),
            Err(errors) => Response::from_errors(errors),
        }
    }

    /// Execute a GraphQL batch query.
    pub async fn execute_batch(&self, batch_request: BatchRequest) -> BatchResponse {
        match batch_request {
            BatchRequest::Single(request) => BatchResponse::Single(self.execute(request).await),
            BatchRequest::Batch(requests) => BatchResponse::Batch(
                futures_util::stream::iter(requests.into_iter())
                    .then(|request| self.execute(request))
                    .collect()
                    .await,
            ),
        }
    }

    pub(crate) fn execute_stream_with_ctx_data(
        &self,
        request: impl Into<Request> + Send,
        ctx_data: Arc<Data>,
    ) -> impl Stream<Item = Response> + Send {
        let schema = self.clone();

        async_stream::stream! {
                    let request = request.into();
                    let (mut env, cache_control) = match schema.prepare_request(request).await {
                        Ok(res) => res,
                        Err(errors) => {
                            yield Response::from_errors(errors);
                            return;
                        }
                    };
                    env.ctx_data = ctx_data;
                    let env = QueryEnv::new(env);

                    if env.operation.node.ty != OperationType::Subscription {
                        yield schema
                            .execute_once(env)
                            .await
                            .cache_control(cache_control);
                        return;
                    }

                    let resolve_id = AtomicUsize::default();
                    let ctx = env.create_context(
                        &schema.env,
                        None,
                        &env.operation.node.selection_set,
                        ResolveId::root(),
                        &resolve_id,
                    );
                    let ctx_extension = ExtensionContext {
                        schema_data: &schema.env.data,
                        query_data: &env.ctx_data,
                    };

                    env.extensions.execution_start(&ctx_extension);

                    /*let mut streams = Vec::new();
                    if let Err(e) = collect_subscription_streams(&ctx, &schema.subscription, &mut streams) {
                        env.extensions.execution_end(&ctx_extension);
                        yield Response::from_errors(vec![e]);
                        return;
                    }*/

                    env.extensions.execution_end(&ctx_extension);
        /*
                    let mut stream = stream::select_all(streams);
                    while let Some(data) = stream.next().await {
                        let is_err = data.is_err();
                        let extensions = env.extensions.result(&ctx_extension);
                        yield match data {
                            Ok((name, value)) => {
                                let mut map = BTreeMap::new();
                                map.insert(name, value);
                                Response::new(Value::Object(map))
                            },
                            Err(e) => Response::from_errors(vec![e]),
                        }.extensions(extensions);
                        if is_err {
                            break;
                        }
                    }*/
                }
    }

    /// Execute a GraphQL subscription.
    pub fn execute_stream(
        &self,
        request: impl Into<Request>,
    ) -> impl Stream<Item = Response> + Send {
        let mut request = request.into();
        let ctx_data = std::mem::take(&mut request.data);
        self.execute_stream_with_ctx_data(request, Arc::new(ctx_data))
    }
}
