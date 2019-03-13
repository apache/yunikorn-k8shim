import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { SchedulerInfo, QueueInfo } from '@app/models/queue-info.model';
import { EnvconfigService } from '../envconfig/envconfig.service';
import { ClusterInfo } from '@app/models/cluster-info.model';
import { DEFAULT_PARTITION_VALUE } from '@app/util/constants';
import { CommonUtils } from '@app/util/common.util';

const SCHEDULER_JSON_URL = './assets/data/scheduler.json';
const QUEUES_JSON_URL = './assets/data/queues.json';
const CLUSTERS_JSON_URL = './assets/data/clusters.json';

interface ResourceInfo {
    memory: string;
    vcore: string;
}

// default: false
const isDevMode = false;

@Injectable({
    providedIn: 'root'
})
export class SchedulerService {
    constructor(private httpClient: HttpClient, private envConfig: EnvconfigService) {}

    public fetchClusterList(): Observable<ClusterInfo[]> {
        const clusterUrl = isDevMode
            ? CLUSTERS_JSON_URL
            : `${this.envConfig.getUschedulerWebAddress()}/ws/v1/clusters`;
        return this.httpClient.get(clusterUrl).pipe(map(data => data as ClusterInfo[]));
    }

    public fetchClusterByName(clusterName: string): Observable<ClusterInfo> {
        return this.fetchClusterList().pipe(
            map(data => {
                return data.find(obj => obj.clustername === clusterName);
            })
        );
    }

    public fetchSchedulerQueues(): Observable<any> {
        const queuesUrl = isDevMode
            ? QUEUES_JSON_URL
            : `${this.envConfig.getUschedulerWebAddress()}/ws/v1/queues`;
        return this.httpClient.get(queuesUrl).pipe(
            map((data: any) => {
                let rootQueue = new QueueInfo();
                if (data && data.queues && data.queues[0]) {
                    const rootQueueData = data.queues[0];
                    rootQueue.queueName = rootQueueData.queuename;
                    rootQueue.state = rootQueueData.status || 'RUNNING';
                    rootQueue.children = null;
                    rootQueue.isLeafQueue = false;
                    this.fillQueueCapacities(rootQueueData, rootQueue);
                    rootQueue = this.generateQueuesTree(rootQueueData, rootQueue);
                }
                return {
                    rootQueue
                };
            })
        );
    }

    private generateQueuesTree(data: any, currentQueue: QueueInfo) {
        if (data && data.queues && data.queues.length > 0) {
            const chilrenQs = [];
            data.queues.forEach(queue => {
                const childQueue = new QueueInfo();
                childQueue.queueName = '' + queue.queuename;
                childQueue.state = queue.status || 'RUNNING';
                childQueue.parentQueue = currentQueue ? currentQueue : null;
                this.fillQueueCapacities(queue, childQueue);
                chilrenQs.push(childQueue);
                return this.generateQueuesTree(queue, childQueue);
            });
            currentQueue.children = chilrenQs;
            currentQueue.isLeafQueue = false;
        } else {
            currentQueue.isLeafQueue = true;
        }
        return currentQueue;
    }

    private fillQueueCapacities(data: any, queue: QueueInfo) {
        const configCap = data['capacities']['capacity'];
        const usedCap = data['capacities']['usedcapacity'];
        const maxCap = data['capacities']['maxcapacity'];

        const configCapResources = this.splitCapacity(configCap);
        const usedCapResources = this.splitCapacity(usedCap);
        const maxCapResources = this.splitCapacity(maxCap);

        const absoluteUsedCapPercent = Math.max(
            Math.round((+usedCapResources.memory / +configCapResources.memory) * 100),
            Math.round((+usedCapResources.vcore / +configCapResources.vcore) * 100)
        );

        queue.capacity = this.formatCapacity(configCapResources) as any;
        queue.maxCapacity = this.formatCapacity(maxCapResources) as any;
        queue.usedCapacity = this.formatCapacity(usedCapResources) as any;
        queue.absoluteUsedCapacity = Math.min(absoluteUsedCapPercent, 100);
    }

    private splitCapacity(capacity: string): ResourceInfo {
        const splitted = capacity.replace(/[\[\]]/g, '').split(' ');
        const resources: ResourceInfo = {
            memory: '0',
            vcore: '0'
        };
        for (const resource of splitted) {
            if (resource) {
                const values = resource.split(':');
                if (values[0] === 'memory') {
                    resources.memory = values[1];
                } else {
                    resources.vcore = values[1];
                }
            }
        }
        return resources;
    }

    private formatCapacity(resourceInfo: ResourceInfo) {
        const formatted = [];
        formatted.push(`memory: ${CommonUtils.formatMemory(+resourceInfo.memory)}`);
        formatted.push(`vcore: ${resourceInfo.vcore}`);
        return formatted.join(', ');
    }

    public fetchSchedulerInfo(): Observable<SchedulerInfo> {
        const schedulerUrl = SCHEDULER_JSON_URL;
        return this.httpClient.get(schedulerUrl).pipe(
            map((data: any) => {
                let rootQueue = this.getRootQueueInfo();
                if (data.scheduler && data.scheduler.schedulerInfo) {
                    const schedInfo = data.scheduler.schedulerInfo;
                    this.addQueueCapacitiesByPartition(schedInfo, rootQueue);
                    rootQueue = this.extractQueues(schedInfo, rootQueue);
                }
                return {
                    rootQueue
                };
            })
        );
    }

    private getRootQueueInfo() {
        const queueInfo = new QueueInfo();
        queueInfo.queueName = 'root';
        queueInfo.state = 'RUNNING';
        queueInfo.capacity = 100;
        queueInfo.maxCapacity = 100;
        queueInfo.usedCapacity = 0;
        queueInfo.absoluteCapacity = 100;
        queueInfo.absoluteMaxCapacity = 100;
        queueInfo.absoluteUsedCapacity = 0;
        queueInfo.effectiveMinMemory = 0;
        queueInfo.effectiveMaxMemory = 0;
        queueInfo.effectiveMinVCores = 0;
        queueInfo.effectiveMaxVCores = 0;
        queueInfo.parentQueue = null;
        queueInfo.queuePath = 'root';
        queueInfo.children = null;
        queueInfo.isLeafQueue = false;
        return queueInfo;
    }

    private addQueueCapacitiesByPartition(queueData: any, queueInfo: QueueInfo) {
        if (queueData.capacities && queueData.capacities.queueCapacitiesByPartition) {
            const partition = queueData.capacities.queueCapacitiesByPartition.find(part => {
                return part.partitionName === DEFAULT_PARTITION_VALUE;
            });
            queueInfo.capacity = partition.capacity || 0;
            queueInfo.maxCapacity = partition.maxCapacity || 0;
            queueInfo.usedCapacity = partition.usedCapacity || 0;
            queueInfo.absoluteCapacity = partition.absoluteCapacity || 0;
            queueInfo.absoluteMaxCapacity = partition.absoluteMaxCapacity || 0;
            queueInfo.absoluteUsedCapacity = partition.absoluteUsedCapacity || 0;
            queueInfo.effectiveMinMemory = partition.effectiveMinResource.memory || 0;
            queueInfo.effectiveMinVCores = partition.effectiveMinResource.vCores || 0;
            queueInfo.effectiveMaxMemory = partition.effectiveMaxResource.memory || 0;
            queueInfo.effectiveMaxVCores = partition.effectiveMaxResource.vCores || 0;
        }
        return queueInfo;
    }

    private extractQueues(schedulerInfo: any, currentQueue: QueueInfo) {
        if (schedulerInfo.queues && schedulerInfo.queues.queue) {
            const chilrenQs: QueueInfo[] = [];
            schedulerInfo.queues.queue.forEach(queue => {
                const childQueue = this.getNewQueueInfo(queue, currentQueue);
                this.addQueueCapacitiesByPartition(queue, childQueue);
                chilrenQs.push(childQueue);
                return this.extractQueues(queue, childQueue);
            });
            currentQueue.children = chilrenQs;
            currentQueue.isLeafQueue = false;
        } else {
            currentQueue.isLeafQueue = true;
        }
        return currentQueue;
    }

    private getNewQueueInfo(queueData: any, parentQueue: QueueInfo) {
        const queueInfo = new QueueInfo();
        queueInfo.queueName = '' + queueData.queueName;
        queueInfo.state = queueData.state || 'RUNNING';
        queueInfo.capacity = queueData.capacity;
        queueInfo.maxCapacity = queueData.maxCapacity;
        queueInfo.usedCapacity = queueData.usedCapacity;
        if (parentQueue) {
            queueInfo.parentQueue = parentQueue;
            queueInfo.queuePath = parentQueue.queuePath + '.' + queueData.queueName;
        } else {
            queueInfo.parentQueue = null;
            queueInfo.queuePath = '' + queueData.queueName;
        }
        queueInfo.children = null;
        queueInfo.isLeafQueue = false;
        return queueInfo;
    }
}
