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

import static com.google.edwmigration.dumper.application.dumper.task.TaskState.FAILED;
import static com.google.edwmigration.dumper.application.dumper.task.TaskState.SUCCEEDED;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Objects;

@RunWith(JUnit4.class)
public class TaskSetStateTest {

  TaskSetState.Impl state = new TaskSetState.Impl();

  @Test
  public void testConcurrentModification() throws InterruptedException {
    Map<Task<?>, TaskResult<?>> map = state.getTaskResultMap();
    
    CompletableFuture.runAsync(this::run);

    int limit = 20;
    for (Map.Entry<Task<?>, TaskResult<?>> e : map.entrySet()) {
      TimeUnit.MILLISECONDS.sleep(500);
      System.out.println(e.getValue().toString());
      --limit;
      if (limit == 0) break;
    }
  }

  void run() {
    int n = 123456;
    ArrayList<TaskResult<?>> results = new ArrayList<>();
    results.add(new TaskResult<>(SUCCEEDED, 0)); 
    results.add(new TaskResult<>(FAILED, 1)); 
    results.add(new TaskResult<>(SUCCEEDED, 2)); 
    results.add(new TaskResult<>(FAILED, 3));
    int size = results.size();
    for (int i = 0; i < n; i++) {
      int valueIndex = Objects.hashCode(i) % size;
      if (valueIndex < 0) {
        valueIndex += size;
      }
      TaskResult<?> value = results.get(valueIndex);
      state.setTaskResult(new TestTask("abc" + i), value.getState(), value.getValue());
    }
  }

  // implementation is not relevant, as long as equals() works by checking ref equality
  static class TestTask implements Task<Object> {

    private final String va;

    TestTask(String v) {
      va = v;
    }

    @Override
    @Nonnull
    public String getTargetPath() {
      return va;
    }

    @Override
    public Object run(@Nonnull TaskRunContext context) throws Exception {
      return new Object();
    }
  }
}
