/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.runtime.common.metric;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

public class MetricUtilsTest {

  @Test
  public void testEnumIndexAndValue() throws ClassNotFoundException {
    final DataFlowProperty.Value pull = DataFlowProperty.Value.PULL;
    final DataFlowProperty.Value push = DataFlowProperty.Value.PUSH;

    final DataFlowProperty pullEP = DataFlowProperty.of(pull);
    final String epValuePull = MetricUtils.epValueToString(pullEP);
    final String epValueClassPull = pullEP.getValue().getClass().getName();
    // Pull is of ordinal index 0
    Assert.assertEquals(0, Integer.parseInt(epValuePull));

    final DataFlowProperty pushEP = DataFlowProperty.of(push);
    final String epValuePush = MetricUtils.epValueToString(pushEP);
    final String epValueClassPush = pushEP.getValue().getClass().getName();
    // Pull is of ordinal index 0
    Assert.assertEquals(1, Integer.parseInt(epValuePush));

    final Object pull1 = MetricUtils.stringToValue(epValuePull, epValueClassPull);
    Assert.assertEquals(pull, pull1);
    final Object push1 = MetricUtils.stringToValue(epValuePush, epValueClassPush);
    Assert.assertEquals(push, push1);
  }

  @Test
  public void testIntegerBooleanIndexAndValue() throws ClassNotFoundException {
    final Integer one = 1;
    final Integer hundred = 100;

    final ParallelismProperty pEp1 = ParallelismProperty.of(one);
    final String pEp1Value = MetricUtils.epValueToString(pEp1);
    final String pEp1ValueClass = pEp1.getValue().getClass().getName();

    final ParallelismProperty pEp100 = ParallelismProperty.of(hundred);
    final String pEp100Value = MetricUtils.epValueToString(pEp100);
    final String pEp100ValueClass = pEp100.getValue().getClass().getName();

    Assert.assertEquals(1, Integer.parseInt(pEp1Value));
    Assert.assertEquals(100, Integer.parseInt(pEp100Value));


    final ResourceSlotProperty rsEpT = ResourceSlotProperty.of(true);
    final String rsEpTValue = MetricUtils.epValueToString(rsEpT);
    final String rsEpTValueClass = rsEpT.getValue().getClass().getName();

    final ResourceSlotProperty rsEpF = ResourceSlotProperty.of(false);
    final String rsEpFValue = MetricUtils.epValueToString(rsEpF);
    final String rsEpFValueClass = rsEpF.getValue().getClass().getName();

    Assert.assertTrue(Boolean.parseBoolean(rsEpTValue));
    Assert.assertFalse(Boolean.parseBoolean(rsEpFValue));

    final Object one1 = MetricUtils.stringToValue(pEp1Value, pEp1ValueClass);
    Assert.assertEquals(one, one1);

    final Object hundred1 = MetricUtils.stringToValue(pEp100Value, pEp100ValueClass);
    Assert.assertEquals(hundred, hundred1);

    final Object t1 = MetricUtils.stringToValue(rsEpTValue, rsEpTValueClass);
    Assert.assertEquals(true, t1);

    final Object f1 = MetricUtils.stringToValue(rsEpFValue, rsEpFValueClass);
    Assert.assertEquals(false, f1);
  }

  @Test
  public void testOtherIndexAndValue() throws ClassNotFoundException {
    final EncoderFactory ef = new EncoderFactory.DummyEncoderFactory();
    final DecoderFactory df = new DecoderFactory.DummyDecoderFactory();

    final EncoderProperty eEp = EncoderProperty.of(ef);
    final String eEpValue = MetricUtils.epValueToString(eEp);
    final String eEpValueClass = eEp.getValue().getClass().getName();

    final DecoderProperty dEp = DecoderProperty.of(df);
    final String dEpValue = MetricUtils.epValueToString(dEp);
    final String dEpValueClass = dEp.getValue().getClass().getName();

    final Object ef1 = MetricUtils.stringToValue(eEpValue, eEpValueClass);
    Assert.assertEquals(ef.toString(), ef1.toString());

    final Object df1 = MetricUtils.stringToValue(dEpValue, dEpValueClass);
    Assert.assertEquals(df.toString(), df1.toString());
  }

  @Test
  public void testPairAndValueToEP() {
    final DataFlowProperty.Value pull = DataFlowProperty.Value.PULL;
    final DataFlowProperty ep = DataFlowProperty.of(pull);
    final String epKeyClass = ep.getClass().getName();
    final String epValueClass = pull.getClass().getName();
    final String epValue = MetricUtils.epValueToString(ep);
    Assert.assertEquals(0, Integer.parseInt(epValue));

    final ExecutionProperty<? extends Serializable> ep2 =
      MetricUtils.keyAndValueToEP(epKeyClass, epValueClass, epValue);
    Assert.assertEquals(ep, ep2);
  }
}
