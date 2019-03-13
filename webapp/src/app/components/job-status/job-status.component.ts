import { Component, OnInit, Input } from '@angular/core';

import { DonutDataItem } from '@app/models/donut-data.model';

@Component({
    selector: 'app-job-status',
    templateUrl: './job-status.component.html',
    styleUrls: ['./job-status.component.scss']
})
export class JobStatusComponent implements OnInit {
    @Input()
    chartData: DonutDataItem[];

    constructor() {}

    ngOnInit() {}
}
