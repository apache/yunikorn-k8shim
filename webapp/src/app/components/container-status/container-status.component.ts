import { Component, OnInit, Input } from '@angular/core';

import { DonutDataItem } from '@app/models/donut-data.model';

@Component({
    selector: 'app-container-status',
    templateUrl: './container-status.component.html',
    styleUrls: ['./container-status.component.scss']
})
export class ContainerStatusComponent implements OnInit {
    @Input()
    chartData: DonutDataItem[];

    constructor() {}

    ngOnInit() {}
}
