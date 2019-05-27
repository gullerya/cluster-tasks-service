## CTS - examples

### Example A

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
