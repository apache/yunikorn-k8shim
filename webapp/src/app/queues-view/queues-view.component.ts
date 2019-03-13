import { Component, OnInit, ViewChild } from '@angular/core';
import { MatDrawer } from '@angular/material';
import { NgxSpinnerService } from 'ngx-spinner';
import { finalize } from 'rxjs/operators';

import { QueueInfo, ToggleQueueChildrenEvent } from '@app/models/queue-info.model';
import { PartitionInfo } from '@app/models/partition-info.model';
import { SchedulerService } from '@app/services/scheduler/scheduler.service';

interface QueueList {
    [level: string]: QueueInfo[] | null;
}

@Component({
    selector: 'app-queues-view',
    templateUrl: './queues-view.component.html',
    styleUrls: ['./queues-view.component.scss']
})
export class QueuesViewComponent implements OnInit {
    @ViewChild('matDrawer') matDrawer: MatDrawer;

    isDrawerContainerOpen = false;
    partitionSelected = '';
    partitionList: PartitionInfo[] = [];
    rootQueue: QueueInfo = null;
    selectedQueue: QueueInfo = null;
    queueList: QueueList = {};
    queueLevels: string[] = [
        'level_00',
        'level_01',
        'level_02',
        'level_03',
        'level_04',
        'level_05'
    ];

    constructor(private scheduler: SchedulerService, private spinner: NgxSpinnerService) {}

    ngOnInit() {
        this.queueLevels.forEach(level => {
            this.queueList[level] = null;
        });
        this.partitionList = [new PartitionInfo('Default', '')];
        this.spinner.show();
        this.scheduler
            .fetchSchedulerQueues()
            .pipe(
                finalize(() => {
                    this.spinner.hide();
                })
            )
            .subscribe(data => {
                if (data && data.rootQueue) {
                    this.rootQueue = data.rootQueue;
                    this.queueList['level_00'] = [this.rootQueue];
                }
            });
    }

    toggleQueueChildrenView(data: ToggleQueueChildrenEvent) {
        const isExpanded = data.queueItem.isExpanded;
        const children = data.queueItem.children;
        if (isExpanded && data.nextLevel && children) {
            this.queueList[data.nextLevel] = children;
        } else {
            this.queueList[data.nextLevel] = null;
            this.closeQueueRacks(data.nextLevel);
            this.collapseChildrenQueues(data.queueItem);
            this.closeQueueDrawer();
        }
    }

    closeQueueRacks(currentLevel: string) {
        const MAX_LEVELS = 4;
        const level = +currentLevel.split('_')[1];
        for (let index = MAX_LEVELS; index >= level; index--) {
            this.queueList[`level_0${index}`] = null;
        }
    }

    collapseChildrenQueues(queue: QueueInfo) {
        if (queue && queue.children) {
            queue.children.forEach(child => {
                child.isExpanded = false;
                return this.collapseChildrenQueues(child);
            });
        }
    }

    unselectChildrenQueues(queue: QueueInfo, selected: QueueInfo) {
        if (queue !== selected) {
            queue.isSelected = false;
        }
        if (queue && queue.children) {
            queue.children.forEach(child => {
                return this.unselectChildrenQueues(child, selected);
            });
        }
    }

    closeQueueDrawer() {
        if (this.selectedQueue) {
            this.selectedQueue.isSelected = false;
        }
        this.selectedQueue = null;
        this.closeMatDrawer();
    }

    closeMatDrawer() {
        this.matDrawer.close();
        setTimeout(() => {
            this.isDrawerContainerOpen = false;
        }, 100);
    }

    onQueueItemSelected(selected: QueueInfo) {
        this.unselectChildrenQueues(this.rootQueue, selected);
        if (selected.isSelected) {
            this.selectedQueue = selected;
            this.isDrawerContainerOpen = true;
            this.matDrawer.open();
        } else {
            this.selectedQueue = null;
            this.closeMatDrawer();
        }
    }
}
