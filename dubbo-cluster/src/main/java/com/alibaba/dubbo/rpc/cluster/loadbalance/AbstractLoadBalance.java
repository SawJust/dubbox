/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;
import com.logical.router.LogicalConstants;
import com.logical.router.context.LogicalRouterContext;


/**
 * AbstractLoadBalance
 * 
 * @author william.liangf
 */
public abstract class AbstractLoadBalance implements LoadBalance {


    /**
     * 先考虑逻辑链路的路由
     * @param invokers invokers.
     * @param url refer url
     * @param invocation invocation.
     * @param <T>
     * @return
     */
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (invokers == null || invokers.size() == 0) {
            return null;
        }

        String invocationLogicalRouterName = LogicalRouterContext.getInvocationLogicalRouterName();
        boolean hasInvocationLogicalRouterName = StringUtils.isNotEmpty(invocationLogicalRouterName) ? true : false;

        String localLogicalRouterName = LogicalRouterContext.getLocalLogicalRouterName();
        boolean hasLocalLogicalRouterName =  StringUtils.isNotEmpty(localLogicalRouterName) ? true : false;

        if(hasInvocationLogicalRouterName && hasLocalLogicalRouterName && !invocationLogicalRouterName.equals(localLogicalRouterName)){
            throw new RpcException(String.format("Logical router name not Match: invocation=%s, local=%s", invocationLogicalRouterName, localLogicalRouterName));
        }

        if (hasLocalLogicalRouterName) {
            RpcContext.getContext().setLogicalRouterContext(LogicalRouterContext.getInvocationLogicalRouterContext());
        }

        List<Invoker<T>> logicalRouterInvokers = getLogicalRouterInvokers(invocationLogicalRouterName, invokers);
        if (hasInvocationLogicalRouterName && logicalRouterInvokers.size() > 0) {
            // 本服务是逻辑路由服务, 下游包含同名逻辑路由服务
            return (logicalRouterInvokers.size() == 1) ? logicalRouterInvokers.get(0) : doSelect(logicalRouterInvokers, url, invocation);
        } else {
            // 所有非逻辑路由服务
            List<Invoker<T>> noneLogicalInvokers = getNoneLogicalRouterInvokers(invokers);
            return (noneLogicalInvokers.size() == 0) ? null : ((noneLogicalInvokers.size() == 1) ? noneLogicalInvokers.get(0) : doSelect(noneLogicalInvokers, url, invocation));
        }
    }

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
        if (weight > 0) {
	        long timestamp = invoker.getUrl().getParameter(Constants.TIMESTAMP_KEY, 0L);
	    	if (timestamp > 0L) {
	    		int uptime = (int) (System.currentTimeMillis() - timestamp);
	    		int warmup = invoker.getUrl().getParameter(Constants.WARMUP_KEY, Constants.DEFAULT_WARMUP);
	    		if (uptime > 0 && uptime < warmup) {
	    			weight = calculateWarmupWeight(uptime, warmup, weight);
	    		}
	    	}
        }
    	return weight;
    }
    
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
    	int ww = (int) ( (float) uptime / ( (float) warmup / (float) weight ) );
    	return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

    /**
     * 获取logicalName同名的逻辑路由invoker
     *
     * @param logicalName
     * @param invokers
     * @param <T>
     * @return
     */
    protected <T> List<Invoker<T>> getLogicalRouterInvokers(String logicalName, List<Invoker<T>> invokers) {
        List<Invoker<T>> logicalInvokers = new ArrayList<Invoker<T>>();
        if (StringUtils.isNotEmpty(logicalName)) {
            for (Invoker<T> invoker : invokers) {
                String ln = invoker.getUrl().getParameter(LogicalConstants.LOGICAL_ROUTER_NAME);
                if (StringUtils.isNotEmpty(ln) && logicalName.equals(ln)) {
                    logicalInvokers.add(invoker);
                }
            }
        }
        return logicalInvokers;
    }

    /**
     * 获取非逻辑路由invoker
     *
     * @param invokers
     * @param <T>
     * @return
     */
    protected <T> List<Invoker<T>> getNoneLogicalRouterInvokers(List<Invoker<T>> invokers) {
        List<Invoker<T>> noneLogicalInvokers = new ArrayList<Invoker<T>>();
        for (Invoker<T> invoker : invokers) {
            String ln = invoker.getUrl().getParameter(LogicalConstants.LOGICAL_ROUTER_NAME);
            if (StringUtils.isEmpty(ln)) {
                noneLogicalInvokers.add(invoker);
            }
        }
        return noneLogicalInvokers;
    }

}