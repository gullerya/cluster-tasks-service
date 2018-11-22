/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.dto;

import com.microfocus.cluster.tasks.api.enums.ClusterTaskInsertStatus;

/**
 * Created by gullery on 16/12/2016.
 *
 * DTO describing the result of single task persistence attempt into the data storage
 */

public interface ClusterTaskPersistenceResult {
	ClusterTaskInsertStatus getStatus();
}
