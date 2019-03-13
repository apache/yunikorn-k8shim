export interface ClusterInfo {
    clustername: string;
    activeNodes: string;
    failedNodes: string;
    totalNodes: string;
    completedJobs: string;
    failedJobs: string;
    pendingJobs: string;
    runningJobs: string;
    totalJobs: string;
    failedContainers: string;
    pendingContainers: string;
    runningContainers: string;
    totalContainers: string;
}
