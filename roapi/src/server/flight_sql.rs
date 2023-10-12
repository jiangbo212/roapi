use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{Action, ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::sql::{ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest, ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTables, CommandGetTableTypes, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::sql::metadata::{GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder};
use arrow_flight::sql::server::{FlightSqlService};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use prost::Message;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, Streaming};
use tonic::transport::Server;
use uuid::Uuid;
use columnq::arrow::datatypes::Schema;
use columnq::arrow::error::ArrowError::SchemaError;
use columnq::arrow::ipc::writer::IpcWriteOptions;
use columnq::arrow::record_batch::RecordBatch;
use columnq::datafusion::logical_expr::LogicalPlan;
use columnq::datafusion::prelude::DataFrame;
use crate::config::Config;
use crate::context::RoapiContext;
use crate::server::RunnableServer;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

pub struct FlightSql<H: RoapiContext> {
    pub ctx: Arc<H>,
    pub addr: std::net::SocketAddr,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Vec<RecordBatch>>>,
}

impl<H: RoapiContext> FlightSql<H> {
    pub async fn new(ctx: Arc<H>, config: &Config, default_host: String) -> Self {
        let default_addr = format!("{default_host}:50051");

        let addr = config
            .addr
            .flight_sql_server
            .clone()
            .unwrap_or_else(|| default_addr.to_string());

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind address for Flight Sql server");

        Self {
            ctx,
            addr: listener
                .local_addr()
                .expect("Failed to get address from listener"),
            statements: Default::default(),
            results: Default::default(),
        }
    }

    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    fn get_result(&self, handle: &str) -> Result<Vec<RecordBatch>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
        }
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(&handle.to_string());
        Ok(())
    }
}

#[async_trait]
impl<H: RoapiContext> RunnableServer for FlightSql<H> {
    fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn run(&self) -> anyhow::Result<()> {

        let addr = self.addr;

        let service = FlightSql {
            ctx: self.ctx.clone(),
            addr,
            statements: self.statements.clone(),
            results: self.results.clone(),
        };
        let svc = FlightServiceServer::new(service);

        Server::builder().add_service(svc).serve(addr).await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl<H: RoapiContext> FlightSqlService for FlightSql<H> {
    type FlightService = FlightSql<H>;

    async fn do_handshake(&self, _request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Pin<Box<dyn Stream<Item=Result<HandshakeResponse, Status>> + Send>>>, Status> {
        info!("do_handshake");
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        Ok(resp)
    }

    async fn do_get_fallback(&self, _request: Request<Ticket>, message: Any) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let fr: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let handle = fr.handle;

        info!("getting results for {handle}");
        let result = self.get_result(&handle)?;
        // if we get an empty result, create an empty schema
        let (schema, batches) = match result.get(0) {
            None => (Arc::new(Schema::empty()), Vec::<RecordBatch>::new()),
            Some(batch) => (batch.schema(), result.clone()),
        };

        let batch_stream = futures::stream::iter(batches).map(Ok);

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(&self, _query: CommandStatementQuery, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_substrait_plan(&self, _query: CommandStatementSubstraitPlan, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_prepared_statement(&self, cmd: CommandPreparedStatementQuery, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_prepared_statement");
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse uuid", e))?;

        let plan = self.get_plan(handle)?;

        let state = self.ctx.state().await;
        let df = DataFrame::new(state, plan);
        let result = df
            .collect()
            .await
            .map_err(|e| status!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(&self, _query: CommandGetCatalogs, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_catalogs");
        let handle = Uuid::new_v4().hyphenated().to_string();

        let mut builder = GetCatalogsBuilder::new();

        let catalog_names = self.ctx.catalogs().await;
        for catalog_name in catalog_names {
            let _ = builder.append(catalog_name);
        }

        let result = vec![builder.build()?];

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_schemas(&self, cmd: CommandGetDbSchemas, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_schemas");
        let handle = Uuid::new_v4().hyphenated().to_string();

        let mut builder = GetDbSchemasBuilder::new(None::<String>, None::<String>);

        let catalog_name = &cmd.catalog.ok_or(status!("not found catalog", SchemaError("not found catalog".to_string())))?;

        let catalog = self.ctx.catalog(&catalog_name.as_str()).await;

        let catalog = catalog.ok_or(status!("not found catalog", SchemaError("not found catalog".to_string())))?;
        let schema_names = &catalog.schema_names();
        for schema_name in schema_names {
            let _ = builder.append(&catalog_name, schema_name);
        }

        let result = vec![builder.build()?];

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_tables(&self, cmd: CommandGetTables, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables, query:{:?}", &cmd.catalog);
        let handle = Uuid::new_v4().hyphenated().to_string();

        let mut builder = GetTablesBuilder::new(
            None::<String>,
            None::<String>,
            None::<String>,
            None::<String>,
            false,
        );

        let catalog_name = &cmd.catalog
            .ok_or(status!("not found catalog", SchemaError("not found catalog".to_string())))?;

        let catalog = self.ctx
            .catalog(catalog_name.as_str())
            .await
            .ok_or(status!("not found catalog", SchemaError("not found catalog".to_string())))?;

        let schema_name = &cmd.db_schema_filter_pattern
            .ok_or(status!("not found schema", SchemaError("not found schema".to_string())))?;

        let schema = catalog.schema(schema_name.as_str())
            .ok_or(status!("not found schema", SchemaError("not found schema".to_string())))?;

        let table_names = schema.table_names();
        for table_name in table_names {
            let _ = &builder.append(catalog_name, &schema_name, &table_name, "TABLE", &Schema::empty());
        }

        let result = vec![builder.build()?];

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_table_types(&self, _query: CommandGetTableTypes, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_sql_info(&self, _query: CommandGetSqlInfo, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_primary_keys(&self, _query: CommandGetPrimaryKeys, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_exported_keys(&self, _query: CommandGetExportedKeys, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_imported_keys(&self, _query: CommandGetImportedKeys, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_cross_reference(&self, _query: CommandGetCrossReference, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_flight_info_xdbc_type_info(&self, _query: CommandGetXdbcTypeInfo, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn do_get_statement(&self, _ticket: TicketStatementQuery, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_prepared_statement(&self, _query: CommandPreparedStatementQuery, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_catalogs(&self, _query: CommandGetCatalogs, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_schemas(&self, _query: CommandGetDbSchemas, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_tables(&self, _query: CommandGetTables, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_table_types(&self, _query: CommandGetTableTypes, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_sql_info(&self, _query: CommandGetSqlInfo, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_primary_keys(&self, _query: CommandGetPrimaryKeys, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_exported_keys(&self, _query: CommandGetExportedKeys, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_imported_keys(&self, _query: CommandGetImportedKeys, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_cross_reference(&self, _query: CommandGetCrossReference, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_get_xdbc_type_info(&self, _query: CommandGetXdbcTypeInfo, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_put_fallback(&self, _request: Request<Streaming<FlightData>>, _message: Any) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        todo!()
    }

    async fn do_put_statement_update(&self, _ticket: CommandStatementUpdate, _request: Request<Streaming<FlightData>>) -> Result<i64, Status> {
        todo!()
    }

    async fn do_put_prepared_statement_query(&self, _query: CommandPreparedStatementQuery, _request: Request<Streaming<FlightData>>) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        todo!()
    }

    async fn do_put_prepared_statement_update(&self, _query: CommandPreparedStatementUpdate, _request: Request<Streaming<FlightData>>) -> Result<i64, Status> {
        todo!()
    }

    async fn do_put_substrait_plan(&self, _query: CommandStatementSubstraitPlan, _request: Request<Streaming<FlightData>>) -> Result<i64, Status> {
        todo!()
    }

    async fn do_action_fallback(&self, _request: Request<Action>) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        todo!()
    }

    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        todo!()
    }

    async fn do_action_create_prepared_statement(&self, query: ActionCreatePreparedStatementRequest, _request: Request<Action>) -> Result<ActionCreatePreparedStatementResult, Status> {
        let user_query = query.query.as_str();
        info!("do_action_create_prepared_statement: {user_query}");

        let plan = self.ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        self.statements.insert(plan_uuid.clone(), plan.clone());

        let plan_schema = plan.schema();

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(&self, handle: ActionClosePreparedStatementRequest, _request: Request<Action>) -> Result<(), Status> {
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(&self, _query: ActionCreatePreparedSubstraitPlanRequest, _request: Request<Action>) -> Result<ActionCreatePreparedStatementResult, Status> {
        todo!()
    }

    async fn do_action_begin_transaction(&self, _query: ActionBeginTransactionRequest, _request: Request<Action>) -> Result<ActionBeginTransactionResult, Status> {
        todo!()
    }

    async fn do_action_end_transaction(&self, _query: ActionEndTransactionRequest, _request: Request<Action>) -> Result<(), Status> {
        todo!()
    }

    async fn do_action_begin_savepoint(&self, _query: ActionBeginSavepointRequest, _request: Request<Action>) -> Result<ActionBeginSavepointResult, Status> {
        todo!()
    }

    async fn do_action_end_savepoint(&self, _query: ActionEndSavepointRequest, _request: Request<Action>) -> Result<(), Status> {
        todo!()
    }

    async fn do_action_cancel_query(&self, _query: ActionCancelQueryRequest, _request: Request<Action>) -> Result<ActionCancelQueryResult, Status> {
        todo!()
    }

    async fn do_exchange_fallback(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        todo!()
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        todo!()
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/datafusion.example.com.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}