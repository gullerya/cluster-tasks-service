### CTS - configurer SPI general overview

`CTS` configuration design pattern is the SPI pattern.
Consuming/hosting application is expected to implement `ClusterTasksServiceConfigurerSPI` and add this implementation to the Spring context.

`CTS` will wire the configurer, validate it and use it to get any required information provided by consuming application.

If configurer is not available or some of essential data is missing `CTS` will fail-fast (failing Spring context loading).

### SPIs overview

* `CompletableFuture<Boolean> getConfigReadyLatch()` - __optional__, default implementation returns `null`
    > Readiness latch allows consuming application to control exactly when `CTS` may start it's routine:
    > - if the future will resolve with `true`, `CTS` will run normally
    > - if the future will resolve with `false` or throw, `CTS` will halt, logging the error
    > - if the future will not resolve, `CTS` will hang forever waiting for resolution
    >
    > In case the SPI returns `null`, `CTS` considers it as an immediate truthy resolution.


* `DataSource getDataSource()` - __required__
    > Must provide properly configured `DataSource` object, pointing to the relevant schema.
    This is the data source that will be used to manage all `DB` typed tasks. 


* `DataSource getAdministrativeDataSource()` - __optional__, default implementation returns `null`
    > Administrative data source, if/when provided, will be used for auto schema management.
    Therefore it is either the consuming application responsibility to provide a correct schema for the `CTS`, or let `CTS` to manage it by providing relevant data source in this SPI.
    The user configured in this data source MUST be privileged enough to perform schema changes. 


* `DBType getDbType()` - __required__
    > One of the supported DB types. This will be assumed the DB type, that is referred to in the `DataSource` object/s provided by `getDataSource` and, optionally, `getAdministrativeDataSource` SPI/s.


* `boolean isEnabled()` - __optional__, default implementation returns `true`
    > Allows consuming application to control `CTS`'s execution at 'runtime'.
    `CTS` calls this SPI each dispatch and maintenance cycle, meaning effective, that consumer may stop or resume the queue within at most 1 second.
    Pay attention to the following details:
    > - only the current `CTS` instance is stopped, other instances (running on other `JVM`s or even in another Spring context on the same `JVM`) will not be affected
    > - this SPI is expected to run as fast as possible, it is called on the thread of the main event loops (dispatch, maintenance), thus directly affecting the speed of the queue; `CTS` will measure this call duration among other 'foreign' calls (see [monitoring](monitoring.md) documentation)
    