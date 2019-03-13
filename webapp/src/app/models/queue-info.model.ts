export class QueueInfo {
    queueName: string;
    state: 'RUNNING' | 'STOPPED';
    capacity: number;
    maxCapacity: number;
    usedCapacity: number;
    absoluteCapacity: number;
    absoluteMaxCapacity: number;
    absoluteUsedCapacity: number;
    effectiveMinMemory: number;
    effectiveMinVCores: number;
    effectiveMaxMemory: number;
    effectiveMaxVCores: number;
    queuePath: string;
    parentQueue: null | QueueInfo;
    children: null | QueueInfo[];
    isLeafQueue: boolean;
    isExpanded = false;
    isSelected = false;
}

export interface SchedulerInfo {
    rootQueue: QueueInfo;
}

export interface ToggleQueueChildrenEvent {
    queueItem: QueueInfo;
    nextLevel: string;
}
