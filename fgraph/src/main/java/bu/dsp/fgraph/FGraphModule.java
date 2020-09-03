/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import bu.dsp.misc.FGraphConfig;

import java.util.Map;

import com.google.auto.service.AutoService;

@AutoService(StatefulFunctionModule.class)
public final class FGraphModule implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        FGraphConfig.setParallelism(Integer.parseInt(globalConfiguration.get("parallelism")));
        
        FGraphEdgeRouter fEdgeRouter = new FGraphEdgeRouter();
        FGraphQueryRouter fQueryRouter = new FGraphQueryRouter();

        binder.bindIngressRouter(FGraphConstants.REQUEST_INGRESS_EDEG, fEdgeRouter);
        binder.bindIngressRouter(FGraphConstants.REQUEST_INGRESS_QUERY, fQueryRouter);
        binder.bindFunctionProvider(FGraphConstants.FGRAPH_FUNCTION_TYPE, unused -> new FGraphMatchFunction());
    }
}
