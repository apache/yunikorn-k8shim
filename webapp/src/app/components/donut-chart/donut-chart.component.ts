import { Component, OnInit, AfterViewInit, Input, OnChanges, SimpleChanges } from '@angular/core';
import * as Highcharts from 'highcharts';

import { CommonUtils } from '@app/util/common.util';
import { DonutDataItem } from '@app/models/donut-data.model';

@Component({
    selector: 'app-donut-chart',
    templateUrl: './donut-chart.component.html',
    styleUrls: ['./donut-chart.component.scss']
})
export class DonutChartComponent implements OnInit, AfterViewInit, OnChanges {
    chartContainerId: string;

    @Input() data: DonutDataItem[] = [];
    @Input() width = '200px';
    @Input() height = '200px';

    constructor() {}

    ngOnInit() {
        this.chartContainerId = CommonUtils.createUniqId('donut_chart_');
    }

    ngAfterViewInit() {
        if (this.data) {
            this.renderChart(this.data);
        }
    }

    ngOnChanges(changes: SimpleChanges) {
        if (changes.data && changes.data.currentValue && changes.data.currentValue.length > 0) {
            this.renderChart(changes.data.currentValue);
        }
    }

    renderChart(chartData = []) {
        if (!this.chartContainerId) {
            return;
        }
        Highcharts.chart(this.chartContainerId, {
            chart: {
                type: 'pie',
                marginRight: 0,
                marginLeft: 0,
                spacing: [0, 0, 0, 0]
            },
            title: {
                text: ''
            },
            yAxis: {
                title: {
                    text: ''
                }
            },
            tooltip: {
                formatter: function() {
                    return `<b>${this.point.name}</b>: ${this.y}`;
                }
            },
            legend: {
                enabled: false
            },
            exporting: {
                enabled: false
            },
            credits: {
                enabled: false
            },
            plotOptions: {
                pie: {
                    shadow: false,
                    size: '100%',
                    center: ['50%', '50%'],
                    innerSize: '55%',
                    showInLegend: true,
                    dataLabels: {
                        enabled: false
                    }
                }
            },
            series: [
                {
                    data: chartData
                }
            ]
        });
    }
}
