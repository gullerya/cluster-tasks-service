###Summary

`cluster-tasks-service` library is built to provide distribution of tasks across a clustered environment.
Beside providing basic queue functionality in a clustered environment, `cluster-tasks-service` (henceforth `CTS`) employs several advanced distribution features.

Most significant feature, the one that `CTS` was originally written for, is an ability to control tasks processing in a _channelled_ fashion, where only a single task from a specific _channel_ will run at any given moment in the whole cluster.


###Importing and initialization of the service

`CTS` is Spring oriented. Please follow the steps below to start hacking around with it:

- Setup the dependency in your `pom.xml` (as in example below) or other compatible build tool configuration (providing the correct version, of course):
```
<dependency>
    <artifactId>cluster-tasks-service</artifactId>
	<groupId>com.microfocus.octane</groupId>
	<version>${relevant.cts.version}</version>
</dependency>
```

- Implement `ClusterTasksServiceConfigurerSPI` interface as a __Spring__ bean (singleton).
Upon application start, `CTS` will wire this bean and take everything it needs from it (DataSource object, for example).
See [CTS configuration SPI overview](cts-configurer-spi.md) for more details.
  
If all gone smooth, `CTS` will kick in upon the application start and be available for submitting and processing tasks.
Let's review a simple example of usage.

###Basic concepts and usage example

The world of `CTS` may roughly be separated into two:
- the __service__ is responsible for its environment setup, ongoing work and maintenance: collecting and registering processors, pulling and handing tasks over, statistics and monitoring, GC of finished/dead tasks etc. Service also provides few generic API for the consumer, for example API to enqueue tasks.
- a __processors__, implemented by consumer extending appropriate base abstract classes, are the actual tasks processor with custom business logic, almost completely transparent to the framework.

In order to obtain the __service__, wire into your Spring eco-system the interface `ClusterTasksService`. It is a singleton, so the best practice would be to (auto)wire it as a private member of any of your services/components using it.
Detailed info on __service__'s APIs available [here](cts-service-api.md).

Next, you'd like to drop you actual logic for some task processing. To do that, you'll need to implement on of the task processor's base classes and expose them as a Spring beans.
Your processor typically would look like this:
```
public class ClusterTasksProcessorA_test extends ClusterTasksProcessorDefault {
	public final Map<String, Timestamp> tasksProcessed = new LinkedHashMap<>();

	protected ClusterTasksProcessorA_test() {
		super(ClusterTasksDataProviderType.DB, 3);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), new Timestamp(System.currentTimeMillis()));
	}
}
```