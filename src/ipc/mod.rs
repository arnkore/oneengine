/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! IPC模块
//! 
//! 提供Arrow IPC、Flight Exchange、Flight Server等进程间通信功能

pub mod arrow_ipc;
pub mod flight_exchange;
pub mod flight_server;

// 重新导出常用类型
pub use arrow_ipc::{ArrowIpcWriter, ArrowIpcReader, ArrowIpcSpillManager};
pub use flight_exchange::{FlightExchangeServer, FlightExchangeConfig, CreditConfig, CreditState, StreamState, FlightExchangeState, FlightClient};
pub use flight_server::{OneEngineFlightService, FlightServerState, FlightServerConfig, FlightClient as FlightServerClient, FlightClientConfig};
