import { Component, OnInit } from '@angular/core';

@Component({
    selector: 'app-container-history',
    templateUrl: './container-history.component.html',
    styleUrls: ['./container-history.component.scss']
})
export class ContainerHistoryComponent implements OnInit {
    chartData = [55, 35, 30, 30, 19, 68, 42, 30, 65, 40, 30, 29, 18, 28, 22, 30, 44, 18, 34, 59, 47, 38, 70, 48];

    constructor() {}

    ngOnInit() {}
}
