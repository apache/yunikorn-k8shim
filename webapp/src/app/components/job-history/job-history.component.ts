import { Component, OnInit } from '@angular/core';

@Component({
    selector: 'app-job-history',
    templateUrl: './job-history.component.html',
    styleUrls: ['./job-history.component.scss']
})
export class JobHistoryComponent implements OnInit {
    chartData = [19, 29, 18, 28, 22, 30, 18, 65, 40, 30, 70, 34, 59, 47, 38, 68, 42, 30, 48, 55, 35, 30, 30, 44];

    constructor() {}

    ngOnInit() {}
}
