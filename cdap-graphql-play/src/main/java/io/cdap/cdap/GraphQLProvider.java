/*
 *
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;

import javax.annotation.PostConstruct;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

/**
 * TODO
 */
@Component
public class GraphQLProvider {

  @Autowired
  GraphQLDataFetchers graphQLDataFetchers;

  private GraphQL graphQL;

  @PostConstruct
  public void init() throws IOException {
    URL url = Resources.getResource("schema.graphqls");
    String sdl = Resources.toString(url, Charsets.UTF_8);
    GraphQLSchema graphQLSchema = buildSchema(sdl);
    this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
  }

  private GraphQLSchema buildSchema(String sdl) {
    TypeDefinitionRegistry typeDefinitionRegistry = new SchemaParser().parse(sdl);
    RuntimeWiring runtimeWiring = buildWiring();
    SchemaGenerator schemaGenerator = new SchemaGenerator();

    return schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
  }

  private RuntimeWiring buildWiring() {
    return RuntimeWiring.newRuntimeWiring()
      .type(queryTypeRuntimeWiring())
      .type(bookTypeRuntimeWiring())
      .build();
  }

  private TypeRuntimeWiring.Builder bookTypeRuntimeWiring() {
    return newTypeWiring("Book")
      .dataFetcher("author", graphQLDataFetchers.getAuthorDataFetcher());
  }

  private TypeRuntimeWiring.Builder queryTypeRuntimeWiring() {
    return newTypeWiring("Query")
      .dataFetcher("bookById", graphQLDataFetchers.getBookByIdDataFetcher());
  }

  @Bean
  public GraphQL getGraphQL() {
    return graphQL;
  }
}
