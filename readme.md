[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/com.microfocus/cluster-tasks-service.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.microfocus%22%20AND%20a:%22cluster-tasks-service%22)
[![Travis branch](https://img.shields.io/travis/MicroFocus/cluster-tasks-service/master.svg?logo=travis)](https://travis-ci.org/MicroFocus/cluster-tasks-service/branches)
[![AppVeyor branch](https://img.shields.io/appveyor/ci/gullerya/cluster-tasks-service/master.svg?logo=appveyor)](https://ci.appveyor.com/project/gullerya/cluster-tasks-service/branch/master)
[![codecov](https://codecov.io/gh/MicroFocus/cluster-tasks-service/branch/master/graph/badge.svg)](https://codecov.io/gh/MicroFocus/cluster-tasks-service)

## Summary

`cluster-tasks-service` library is built to provide distribution of tasks across a clustered environment.
Beside providing basic queue functionality in a clustered environment, `cluster-tasks-service` (henceforth `CTS`) employs several advanced distribution features.

Most significant feature, the one that `CTS` was originally written for, is an ability to control tasks processing in a __channelled__ fashion, where only a single task from a specific __channel__ will run at any given moment in the whole cluster.

##### Documentation TOC:
- [Configurer SPI](docs/cts-configurer-spi.md) - wiring library into consuming application
- [CTS service](docs/cts-service-api.md) - main library entry point
- [Task processors](docs/cts-task-processors.md) - types and extensibility points
- [Tasks](docs/cts-tasks-overview.md) - types, attributes, behaviors
- [Monitoring](docs/monitoring.md) - metrics and logs
- [Changelog](docs/changelog.md) 

### Importing and initialization of the service (done once per application instance)

`CTS` is Spring based. Please follow the steps below to plug the library into your application lifecycle:

1. Setup the dependency in your `pom.xml` (as in example below) or other compatible build tool configuration (you may review the [Changelog](docs/changelog.md) for an available versions):
    ```
    <dependency>
        <artifactId>cluster-tasks-service</artifactId>
        <groupId>com.microfocus</groupId>
        <version>${relevant.cts.version}</version>
    </dependency>
    ```

2. Make `CTS`'s Spring beans available to your application. This could be done by either adding `cluster-tasks-service-context.xml` file to the list of your context path's in Java driven context initialization, or importing it from one of your own Spring XMLs, like in example below:
    ```
    <import resource="classpath:cluster-tasks-service-context.xml"/>
    ```

3. Implement `ClusterTasksServiceConfigurerSPI` interface and define your implementation as a __Spring__ bean (singleton).
Upon application start, `CTS` will wire this bean and take everything it needs from it (DataSource object, for example).
See [CTS configuration SPI overview](docs/cts-configurer-spi.md) for more details.
  
If these steps done right, `CTS` will kick in upon the application start and be available for submitting and processing tasks.
Let's review a simple example of usage.

### Basic concepts and usage example

The world of `CTS` may roughly be separated into two:
- the __service__ is responsible for its environment setup, ongoing work and maintenance: collecting and registering processors, pulling and handing tasks over, statistics and monitoring, maintenance of finished/dead tasks etc.
 Service also provides few generic API for the consumer, for example API to enqueue tasks.
 Service import/bootstrapping is a __one-time__ effort per application.
- a __processors__, implemented by library consumer and extending appropriate base abstract class, are an actual tasks processors with custom business logic, almost completely transparent to the framework.
 It is likely, that there will be many __processors__ (CPTs) in your application, each handling specific use-case.
 Adding processors is an ongoing effort, aligned with the application evolution.

In order to obtain the __service__, wire into your Spring eco-system the interface `ClusterTasksService`. It is a singleton, so the best practice would be to (auto)wire it as a private member of any of your services/components using it.
Detailed info on __service__'s API available [here](docs/cts-service-api.md).

Next, you'd like to drop you actual logic for some task processing. To do that, you'll need to implement one of the task processor's base classes and expose them as a Spring beans.
Your processor typically would look like this:

```
@Component
public class HeavyRecalcAsyncCTP extends ClusterTasksProcessorSimple {
	private static List<Long> numbers;      //  caution, statics are not thread/scope safe
	private List<String> strings;           //  caution, CTPs are singletons, so this is not thread/scope safe either

	protected HeavyRecalcAsyncCTP() {
		super(ClusterTasksDataProviderType.DB, 3);
	}

	@Override
	public void processTask(ClusterTask task) {
		//  TODO: write your logic here
	}
}
```

<sup><sub>
- Best practice would be to have all your logic encapsulated within the `processTask` method and any helper methods that are called from it, while all of the data sets from both, an original task and of intermediate calculations/transformation/processing - rolling around as local variables/method parameters (pay attention to the thread/scope safety remarks in the example above).
</sub></sup>


To see your __processor__ taking the task, is to enqueue it:

```
@Autowired
private ClusterTasksService clusterTasksService;
...

private void doRecalcOfNewContent(String newContentSerialized) {
	//  prepare task
	ClusterTask task = TaskBuilders.simpleTask()
			.setDelayByMillis(5000L)
			.setBody(newContentSerialized)
			.build();

	//  enqueue task to the DB storage (the only available as of now) and to the correct processor
	ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
		ClusterTasksDataProviderType.DB,
		"HeavyRecalcAsyncCTP",
		task);

	//  you may inspect results of enqueue and process/log/retry any abnormalities
	...
}
```

<sup><sub>
- Obviously, tasks should be submitted to the specific __processor__. To achieve that, you need to specify __processor__'s type (second parameter). By default __processor__'s type is its simple class name, but you can change it overriding `getType` method in your __processor__'s implementation.
</sub></sup>
