use async_graphql_value::ConstValue;

use crate::{
    registry::{MetaType, Registry},
    Context, ServerError,
};

#[doc(hidden)]
pub mod query_root;
//#[doc(hidden)]
//pub mod registry;
#[doc(hidden)]
pub mod schema;

pub async fn resolve_field(
    registry: Registry,
    obj: &MetaType,
    ctx: &Context<'_>,
) -> Result<Option<ConstValue>, ServerError> {
    Ok(Some(ConstValue::Null))
}
