import { Component, OnInit } from '@angular/core';
import { NgxSpinnerService } from 'ngx-spinner';
import { finalize } from 'rxjs/operators';

import { SchedulerService } from '@app/services/scheduler/scheduler.service';
import { ClusterInfo } from '@app/models/cluster-info.model';

@Component({
    selector: 'app-dashboard',
    templateUrl: './dashboard.component.html',
    styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
    clusterList: ClusterInfo[] = [];

    constructor(private scheduler: SchedulerService, private spinner: NgxSpinnerService) {}

    ngOnInit() {
        this.spinner.show();
        this.scheduler
            .fetchClusterList()
            .pipe(
                finalize(() => {
                    this.spinner.hide();
                })
            )
            .subscribe(list => {
                this.clusterList = list;
            });
    }
}
