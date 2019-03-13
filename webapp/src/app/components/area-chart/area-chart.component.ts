import { Component, OnInit, AfterViewInit, Input, OnChanges, SimpleChanges } from '@angular/core';
import * as Highcharts from 'highcharts';

import { CommonUtils } from '@app/util/common.util';
import { TimeSeriesDataItem } from '@app/models/time-series-data.model';

@Component({
    selector: 'app-area-chart',
    templateUrl: './area-chart.component.html',
    styleUrls: ['./area-chart.component.scss']
})
export class AreaChartComponent implements OnInit, AfterViewInit, OnChanges {
    chartContainerId: string;

    // @Input() data: TimeSeriesDataItem[] = [];
    @Input() data: any[] = [];
    @Input() color = '#72bdd7';
    @Input() tooltipLabel = 'Value';

    constructor() {}

    ngOnInit() {
        this.chartContainerId = CommonUtils.createUniqId('area_chart_');
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
        const tooltipLabel = this.tooltipLabel;
        Highcharts.chart(this.chartContainerId, {
            chart: {
                type: 'areaspline',
                marginLeft: 0,
                marginRight: 0
            },
            title: {
                text: ''
            },
            subtitle: {
                text: ''
            },
            xAxis: {
                labels: {
                    enabled: true,
                    distance: 0,
                    formatter: function() {
                        let label = this.axis.defaultLabelFormatter.call(this);
                        if (label === '0') {
                            label = '12am';
                        } else {
                            label = label + 'pm';
                        }
                        return label;
                    }
                },
                tickInterval: 1,
                tickmarkPlacement: 'on',
                title: {
                    enabled: false
                },
                crosshair: {
                    width: 1,
                    color: '#cbd0d2',
                    dashStyle: 'solid',
                    zIndex: 5
                },
                minPadding: 0,
                maxPadding: 0
            },
            yAxis: {
                title: {
                    text: ''
                },
                labels: {
                    enabled: false
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
            tooltip: {
                formatter: function() {
                    return `<b>${tooltipLabel}</b>: ${this.y}`;
                },
                shared: true,
                backgroundColor: '#48484a',
                borderColor: '#48484a',
                borderRadius: 10,
                borderWidth: 1,
                style: {
                    color: '#fff'
                }
            },
            plotOptions: {
                areaspline: {
                    pointStart: 0,
                    pointInterval: 0.5,
                    lineWidth: 2,
                    marker: {
                        enabled: false
                    },
                    color: '#72bdd7',
                    fillColor: '#edf9fb'
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
