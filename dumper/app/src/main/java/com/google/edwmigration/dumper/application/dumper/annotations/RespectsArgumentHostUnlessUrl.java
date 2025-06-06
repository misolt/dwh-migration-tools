/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.annotations;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** @author shevek */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_HOST,
    description = RespectsArgumentHostUnlessUrl.DESCRIPTION,
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = ConnectorArguments.OPT_HOST_DEFAULT)
public @interface RespectsArgumentHostUnlessUrl {

  public static final String DESCRIPTION = "The hostname of the database server.";

  // public static final String EXAMPLES[] = {};
}
