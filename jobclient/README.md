# Cook Java Client

Please run `mvn javadoc:javadoc` to build the docs for this project.
The main entrypoint is `com.twosigma.cook.jobclient`; read the Javadocs for details.

# Example Usage

Submitting two jobs that should run in the same AWS region:

```java
public class HostPlacementExample {
    @Test
    public void twoJobsInTheSameRegion() throws URISyntaxException, JobClientException {
        // Create a host placement constraint where the region attribute must equal across hosts
        HostPlacement.Builder hostPlacementBuilder = new HostPlacement.Builder();
        hostPlacementBuilder.setType(HostPlacement.Type.ATTRIBUTE_EQUALS);
        hostPlacementBuilder.setParameter("attribute", "region");
        HostPlacement hostPlacement = hostPlacementBuilder.build();

        // Create a job group with the host placement constraint
        Group.Builder groupBuilder = new Group.Builder();
        groupBuilder.setUUID(UUID.randomUUID());
        groupBuilder.setName("testing");
        groupBuilder.setHostPlacement(hostPlacement);
        Group group = groupBuilder.build();

        // Create two jobs and place them in the job group
        Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setCommand("echo hello");
        jobBuilder.setCpus(1.0);
        jobBuilder.setMemory(128.0);
        jobBuilder.setGroup(group);
        jobBuilder.setUUID(UUID.randomUUID());
        Job job1 = jobBuilder.build();
        jobBuilder.setUUID(UUID.randomUUID());
        Job job2 = jobBuilder.build();

        // Create a job client and submit our jobs and job group
        JobClient.Builder clientBuilder = new JobClient.Builder();
        clientBuilder.setHost("localhost");
        clientBuilder.setPort(12321);
        clientBuilder.setJobEndpoint("rawscheduler");
        JobClient client = clientBuilder.build();
        client.submitWithGroups(Arrays.asList(job1, job2), Collections.singletonList(group));
    }
}
``` 

# Running the Tests

The easiest way to run the JobClient unit tests is to use Maven:

```bash
mvn dependency:resolve
mvn test
```

&copy; Two Sigma Open Source, LLC
