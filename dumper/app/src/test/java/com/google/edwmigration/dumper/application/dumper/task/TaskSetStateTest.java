/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.task;

import static com.google.edwmigration.dumper.application.dumper.task.TaskState.SUCCEEDED;

import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TaskSetStateTest {

  static final int N = 1 << 17;
  final TaskSetState.Impl state = new TaskSetState.Impl();

  // synchronized // uncomment to fix
  void putN() {
    for (int i = 0; i < N; i++) {
      TaskResult<?> result = new TaskResult<>(SUCCEEDED, null);
      // unsafe: usage of getTaskResultMap bypasses the mutex
      state.getTaskResultMap().put(new TestTask(), result);
    }
  }

  @Test
  public void testConcurrentInsert() throws InterruptedException {
    CompletableFuture<Void> future = CompletableFuture.runAsync(this::putN);
    putN();
    future.join();
    int size = state.getTaskResultMap().size();
    String message = String.format("Expected %s entries, got %s", N + N, size);
    // example output: Expected 262144 entries, got 238594
    System.out.println(message);
  }

  // implementation is not relevant, as long as equals() works by checking ref equality
  static class TestTask implements Task<Object> {

    @Override
    @Nonnull
    public String getTargetPath() {
      return "path";
    }

    @Override
    public Object run(@Nonnull TaskRunContext context) throws Exception {
      return new Object();
    }
  }
}
