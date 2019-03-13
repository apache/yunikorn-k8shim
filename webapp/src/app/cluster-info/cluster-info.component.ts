import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { NgxSpinnerService } from 'ngx-spinner';
import { finalize } from 'rxjs/operators';

import { SchedulerService } from '@app/services/scheduler/scheduler.service';
import { ClusterInfo } from '@app/models/cluster-info.model';
import { DonutDataItem } from '@app/models/donut-data.model';

@Component({
    selector: 'app-cluster-info',
    templateUrl: './cluster-info.component.html',
    styleUrls: ['./cluster-info.component.scss']
})
export class ClusterInfoComponent implements OnInit {
    clusterInfo: ClusterInfo = null;
    jobStatusData: DonutDataItem[] = [];
    containerStatusData: DonutDataItem[] = [];

    constructor(
        private scheduler: SchedulerService,
        private route: ActivatedRoute,
        private spinner: NgxSpinnerService
    ) {}

    ngOnInit() {
        const clusterName = this.route.parent.snapshot.params['clusterName'];
        this.spinner.show();
        this.scheduler
            .fetchClusterByName(clusterName)
            .pipe(
                finalize(() => {
                    this.spinner.hide();
                })
            )
            .subscribe(data => {
                this.updateJobStatusData(data);
                this.updateContainerStatusData(data);
            });
    }

    updateJobStatusData(info: ClusterInfo) {
        this.jobStatusData = [
            new DonutDataItem('Failed', +info.failedJobs, '#cc6164'),
            new DonutDataItem('Pending', +info.pendingJobs, '#facc54'),
            new DonutDataItem('Running', +info.runningJobs, '#26bbf0'),
            new DonutDataItem('Completed', +info.runningJobs, '#60cea5')
        ];
    }

    updateContainerStatusData(info: ClusterInfo) {
        this.containerStatusData = [
            new DonutDataItem('Failed', +info.failedContainers, '#cc6164'),
            new DonutDataItem('Pending', +info.pendingContainers, '#facc54'),
            new DonutDataItem('Running', +info.runningContainers, '#26bbf0')
        ];
    }
}
