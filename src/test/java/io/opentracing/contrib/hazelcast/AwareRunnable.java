/*
 * Copyright 2018-2019 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.NodeAware;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;
import java.io.Serializable;
import java.util.Objects;

public class AwareRunnable implements Runnable, Serializable,
    HazelcastInstanceAware, NodeAware, SerializationServiceAware {

  private HazelcastInstance hazelcastInstance;
  private Node node;
  private SerializationService serializationService;

  @Override
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  public void setNode(Node node) {

    this.node = node;
  }

  @Override
  public void setSerializationService(
      SerializationService serializationService) {

    this.serializationService = serializationService;
  }

  @Override
  public void run() {
    Objects.requireNonNull(hazelcastInstance);
    Objects.requireNonNull(node);
    Objects.requireNonNull(serializationService);
  }
}
