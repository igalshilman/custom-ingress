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
package com.igal;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.igal.generated.ProducerRecordMessage;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public final class Module implements StatefulFunctionModule {

  private static final IngressIdentifier<Message> IN =
      new IngressIdentifier<>(Message.class, "foo", "bar");

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {

    binder.bindIngress(
        KafkaIngressBuilder.forIdentifier(IN)
            .withTopic("input-topic")
            .withKafkaAddress("kafka-broker:9092")
            .withDeserializer(MySpecialDeserializer.class)
            // set the relevant properties here
            //  .withProperty() etc'
            .build());

    binder.bindIngressRouter(IN, new MySpecialRouter());
  }

  private static final class MySpecialDeserializer implements KafkaIngressDeserializer<Message> {

    @Override
    public Message deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
      ProducerRecordMessage.Builder builder = ProducerRecordMessage.newBuilder();
      // copy the specific parts that you care about from the record to
      // the builder.
      if (consumerRecord.key() == null) {
        builder.setSkip(true);
        return builder.build();
      }
      builder.setSkip(false);

      ByteString byteString = ByteString.copyFrom(consumerRecord.key());
      builder.setKey(byteString);
      builder.setValueTypeUrl(
          "foo.bar/MyType"); // perhaps you'd like to extract it from the headers? or the topic
      // name?
      builder.setValue(ByteString.copyFrom(consumerRecord.value()));
      builder.addTargetTypeNamespace("namespace");
      builder.addTargetType("target");
      builder.addTargetIdBytes(byteString);
      // chose additional targets. perhaps the information is at the headers? or perhaps hardcoded
      return builder.build();
    }
  }

  private static class MySpecialRouter implements Router<Message> {
    @Override
    public void route(Message message, Downstream<Message> downstream) {
      // message is whatever the custom MySpecialDeserializer has produced.
      // so that would be the place to extract a target information if that is not hardcoded.
      ProducerRecordMessage recordMessage = (ProducerRecordMessage) message;
      if (recordMessage.getSkip()) {
        return;
      }
      int targets = recordMessage.getTargetTypeList().size();
      for (int i = 0; i < targets; i++) {
        String namespace = recordMessage.getTargetTypeNamespace(i);
        String target = recordMessage.getTargetType(i);
        String id = recordMessage.getTargetId(i);

        // the message it self has to be protobuf any.

        downstream.forward(
            new FunctionType(namespace, target),
            id,
            Any.newBuilder()
                .setTypeUrl(recordMessage.getValueTypeUrl())
                .setValue(recordMessage.getValue())
                .build());
      }
    }
  }
}
