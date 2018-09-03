/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.INCIDENT_PROCESSOR_ID;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.WORKFLOW_INSTANCE_PROCESSOR_ID;
import static io.zeebe.broker.workflow.WorkflowServiceNames.WORKFLOW_REPOSITORY_SERVICE;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.incident.processor.IncidentStreamProcessor;
import io.zeebe.broker.logstreams.processor.StreamProcessorIds;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory.Builder;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.transport.controlmessage.ControlMessageHandlerManager;
import io.zeebe.broker.workflow.deployment.distribute.processor.DistributionStreamProcessor;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.WorkflowInstanceStreamProcessor;
import io.zeebe.broker.workflow.repository.GetWorkflowControlMessageHandler;
import io.zeebe.broker.workflow.repository.ListWorkflowsControlMessageHandler;
import io.zeebe.broker.workflow.repository.WorkflowRepositoryService;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.protocol.Protocol;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;

/** Tracks leader partitions and installs the workflow instance stream processors */
public class WorkflowManagerService implements Service<WorkflowManagerService> {

  private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
  private final Injector<ClientTransport> managementApiClientInjector = new Injector<>();
  private final Injector<ClientTransport> subscriptionApiClientInjector = new Injector<>();
  private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
  private final Injector<ControlMessageHandlerManager> controlMessageHandlerManagerServiceInjector =
      new Injector<>();
  private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
      new Injector<>();

  private final ServiceGroupReference<Partition> partitionsGroupReference =
      ServiceGroupReference.<Partition>create()
          .onAdd((partitionName, partition) -> startStreamProcessors(partitionName, partition))
          .build();

  private final BrokerCfg brokerCfg;

  public WorkflowManagerService(final BrokerCfg brokerCfg) {
    this.brokerCfg = brokerCfg;
  }

  private GetWorkflowControlMessageHandler getWorkflowMessageHandler;
  private ListWorkflowsControlMessageHandler listWorkflowsControlMessageHandler;
  private StreamProcessorServiceFactory streamProcessorServiceFactory;
  private ServerTransport clientApiTransport;
  private TopologyManager topologyManager;
  private ServiceStartContext startContext;
  private ClientTransport managementApi;

  @Override
  public void start(final ServiceStartContext serviceContext) {
    this.startContext = serviceContext;
    this.managementApi = managementApiClientInjector.getValue();
    this.clientApiTransport = clientApiTransportInjector.getValue();
    this.streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
    this.topologyManager = topologyManagerInjector.getValue();

    getWorkflowMessageHandler =
        new GetWorkflowControlMessageHandler(clientApiTransport.getOutput());
    listWorkflowsControlMessageHandler =
        new ListWorkflowsControlMessageHandler(clientApiTransport.getOutput());

    final ControlMessageHandlerManager controlMessageHandlerManager =
        controlMessageHandlerManagerServiceInjector.getValue();
    controlMessageHandlerManager.registerHandler(getWorkflowMessageHandler);
    controlMessageHandlerManager.registerHandler(listWorkflowsControlMessageHandler);
  }

  public void startStreamProcessors(
      final ServiceName<Partition> partitionServiceName, final Partition partition) {
    installWorkflowStreamProcessor(partition, partitionServiceName);
    installIncidentStreamProcessor(partition, partitionServiceName);

    if (Protocol.DEPLOYMENT_PARTITION == partition.getInfo().getPartitionId()) {
      installDistributeStreamProcessor(partition, partitionServiceName);
    }
  }

  private void installDistributeStreamProcessor(
      final Partition partition, final ServiceName<Partition> partitionServiceName) {

    final String processorName = "deployment-" + partition.getInfo().getPartitionId();
    final int deploymentProcessorId = StreamProcessorIds.DISTRIBUTE_PROCESSOR_ID;

    final Builder streamProcessorServiceBuilder =
        streamProcessorServiceFactory
            .createService(partition, partitionServiceName)
            .processorId(deploymentProcessorId)
            .processorName(processorName);

    final TypedStreamEnvironment streamEnvironment =
        new TypedStreamEnvironment(partition.getLogStream(), clientApiTransport.getOutput());

    final DistributionStreamProcessor distributionStreamProcessor =
        new DistributionStreamProcessor(brokerCfg.getCluster(), topologyManager, managementApi);

    final StateStorage stateStorage =
        partition.getStateStorageFactory().create(deploymentProcessorId, processorName);
    final StateSnapshotController stateSnapshotController =
        distributionStreamProcessor.createStateSnapshotController(stateStorage);

    streamProcessorServiceBuilder
        .processor(distributionStreamProcessor.createStreamProcessor(streamEnvironment))
        .snapshotController(stateSnapshotController)
        .build();
  }

  private void installWorkflowStreamProcessor(
      final Partition partition, final ServiceName<Partition> partitionServiceName) {
    final ServerTransport transport = clientApiTransportInjector.getValue();

    final WorkflowCache workflowCache = new WorkflowCache();

    final SubscriptionCommandSender subscriptionCommandSender =
        new SubscriptionCommandSender(managementApi, subscriptionApiClientInjector.getValue());

    final WorkflowInstanceStreamProcessor streamProcessor =
        createWorkflowStreamProcessor(
            partition, partitionServiceName, workflowCache, subscriptionCommandSender);
    final TypedStreamEnvironment env =
        new TypedStreamEnvironment(partition.getLogStream(), transport.getOutput());

    streamProcessorServiceFactory
        .createService(partition, partitionServiceName)
        .processor(streamProcessor.createStreamProcessor(env))
        .processorId(WORKFLOW_INSTANCE_PROCESSOR_ID)
        .processorName("workflow-instance")
        .build();
  }

  private WorkflowInstanceStreamProcessor createWorkflowStreamProcessor(
      final Partition partition,
      final ServiceName<Partition> partitionServiceName,
      final WorkflowCache workflowCache,
      final SubscriptionCommandSender subscriptionCommandSender) {
    final WorkflowInstanceStreamProcessor streamProcessor;
    if (Protocol.DEPLOYMENT_PARTITION == partition.getInfo().getPartitionId()) {
      streamProcessor =
          new WorkflowInstanceStreamProcessor(
              ctx -> {
                final WorkflowRepositoryService workflowRepositoryService =
                    new WorkflowRepositoryService(ctx.getActorControl(), workflowCache);

                startContext
                    .createService(WORKFLOW_REPOSITORY_SERVICE, workflowRepositoryService)
                    .dependency(partitionServiceName)
                    .install();

                getWorkflowMessageHandler.setWorkflowRepositoryService(workflowRepositoryService);

                listWorkflowsControlMessageHandler.setWorkflowRepositoryService(
                    workflowRepositoryService);
              },
              () -> {
                getWorkflowMessageHandler.setWorkflowRepositoryService(null);
                listWorkflowsControlMessageHandler.setWorkflowRepositoryService(null);
              },
              workflowCache,
              subscriptionCommandSender,
              topologyManager);
    } else {
      streamProcessor =
          new WorkflowInstanceStreamProcessor(
              workflowCache, subscriptionCommandSender, topologyManager);
    }
    return streamProcessor;
  }

  private void installIncidentStreamProcessor(
      final Partition partition, final ServiceName<Partition> partitionServiceName) {
    final TypedStreamEnvironment env =
        new TypedStreamEnvironment(partition.getLogStream(), clientApiTransport.getOutput());
    final IncidentStreamProcessor incidentProcessorFactory = new IncidentStreamProcessor();

    streamProcessorServiceFactory
        .createService(partition, partitionServiceName)
        .processor(incidentProcessorFactory.createStreamProcessor(env))
        .processorId(INCIDENT_PROCESSOR_ID)
        .processorName("incident")
        .build();
  }

  @Override
  public WorkflowManagerService get() {
    return this;
  }

  public Injector<ServerTransport> getClientApiTransportInjector() {
    return clientApiTransportInjector;
  }

  public ServiceGroupReference<Partition> getPartitionsGroupReference() {
    return partitionsGroupReference;
  }

  public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector() {
    return streamProcessorServiceFactoryInjector;
  }

  public Injector<TopologyManager> getTopologyManagerInjector() {
    return topologyManagerInjector;
  }

  public Injector<ClientTransport> getManagementApiClientInjector() {
    return managementApiClientInjector;
  }

  public Injector<ClientTransport> getSubscriptionApiClientInjector() {
    return subscriptionApiClientInjector;
  }

  public Injector<ControlMessageHandlerManager> getControlMessageHandlerManagerServiceInjector() {
    return controlMessageHandlerManagerServiceInjector;
  }
}
