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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestStructProjection {
  @Test
  public void testSubsetProjection() {
    Schema dataSchema =
        new Schema(
            required(10, "id", Types.LongType.get()),
            required(20, "name", Types.StringType.get()),
            required(30, "value", Types.IntegerType.get()));

    Schema projectedSchema = new Schema(required(30, "value", Types.IntegerType.get()));

    StructProjection projectedStructure = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);

    projectedStructure.wrap(row);

    assertThat(projectedStructure.size()).isEqualTo(1);
    assertThat(projectedStructure.get(0, Integer.class)).isEqualTo(42);
  }

  @Test
  public void testNestedProjection() {
    Schema dataSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "address",
                StructType.of(
                    required(10, "street", Types.StringType.get()),
                    required(
                        20,
                        "coordinates",
                        StructType.of(
                            required(100, "latitude", Types.DoubleType.get()),
                            required(200, "longitude", Types.DoubleType.get()))))));

    StructType projectedCoordinateStruct =
        StructType.of(required(200, "longitude", Types.DoubleType.get()));

    StructType projectedAddressStruct =
        StructType.of(required(20, "coordinates", projectedCoordinateStruct));

    Schema projectedSchema = new Schema(required(2, "address", projectedAddressStruct));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row coordinates = TestHelpers.Row.of(42.00, 24.00);
    TestHelpers.Row address = TestHelpers.Row.of("221B Baker Street", coordinates);
    TestHelpers.Row row = TestHelpers.Row.of(1L, address);

    projection.wrap(row);

    StructLike addressProjection = projection.get(0, StructLike.class);
    assertThat(addressProjection).isNotNull();
    assertThat(addressProjection.size()).isEqualTo(1);

    StructLike coordinatesProjection = addressProjection.get(0, StructLike.class);
    assertThat(coordinatesProjection).isNotNull();
    assertThat(coordinatesProjection.size()).isEqualTo(1);
    assertThat(coordinatesProjection.get(0, Double.class)).isEqualTo(24.00);
  }

  @Test
  public void testAllowMissingOptionalField() {
    Schema dataSchema = new Schema(required(10, "id", Types.LongType.get()));

    StructType projectedStructType =
        StructType.of(
            required(10, "id", Types.LongType.get()),
            Types.NestedField.optional(20, "name", Types.StringType.get()));

    StructProjection projection =
        StructProjection.createAllowMissing(dataSchema.asStruct(), projectedStructType);

    TestHelpers.Row row = TestHelpers.Row.of(1L);
    projection.wrap(row);

    assertThat(projection.get(0, Long.class)).isEqualTo(1L);
    assertThat(projection.get(1, String.class)).isNull();
  }

  @Test
  public void testMapWithNestedValueFullMatch() {
    StructType coordinateStruct =
        StructType.of(
            required(100, "latitude", Types.DoubleType.get()),
            required(200, "longitude", Types.DoubleType.get()));

    Schema dataSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "address",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), coordinateStruct)));
    Schema projectedSchema =
        new Schema(
            required(
                2,
                "address",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), coordinateStruct)));
    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row coordinates = TestHelpers.Row.of(42.00, 24.00);
    Map<String, TestHelpers.Row> location = Map.of("home", coordinates);
    TestHelpers.Row row = TestHelpers.Row.of(1L, location);

    projection.wrap(row);

    Map<String, TestHelpers.Row> projectedLocations = projection.get(0, Map.class);
    assertThat(projectedLocations).isNotNull().containsKey("home");

    TestHelpers.Row projectedCoordinatesRow = projectedLocations.get("home");
    assertThat(projectedCoordinatesRow.get(0, Double.class)).isNotNull().isEqualTo(42.00);
    assertThat(projectedCoordinatesRow.get(1, Double.class)).isNotNull().isEqualTo(24.00);
  }

  @Test
  public void testListWithFullMatch() {
    Schema dataSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "numbers", Types.ListType.ofRequired(3, Types.StringType.get())));
    Schema projectedSchema =
        new Schema(required(2, "numbers", Types.ListType.ofRequired(3, Types.StringType.get())));
    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row row = TestHelpers.Row.of(1L, List.of("a", "b", "c"));
    projection.wrap(row);

    assertThat(projection.get(0, List.class)).containsExactly("a", "b", "c");
  }
}
