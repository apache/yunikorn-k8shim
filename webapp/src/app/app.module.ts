import { NgModule, APP_INITIALIZER } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterModule, Routes } from '@angular/router';
import { HttpClientModule } from '@angular/common/http';
import { NgxSpinnerModule } from 'ngx-spinner';

import {
    MatCardModule,
    MatTabsModule,
    MatSelectModule,
    MatDividerModule,
    MatListModule,
    MatProgressBarModule,
    MatSidenavModule
} from '@angular/material';

import { AppComponent } from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { ClusterInfoComponent } from './cluster-info/cluster-info.component';
import { QueuesViewComponent } from './queues-view/queues-view.component';
import { ClusterContainerComponent } from './cluster-container/cluster-container.component';
import { DonutChartComponent } from './components/donut-chart/donut-chart.component';
import { AreaChartComponent } from './components/area-chart/area-chart.component';
import { JobStatusComponent } from './components/job-status/job-status.component';
import { JobHistoryComponent } from './components/job-history/job-history.component';
import { ContainerStatusComponent } from './components/container-status/container-status.component';
import { ContainerHistoryComponent } from './components/container-history/container-history.component';
import { QueueRackComponent } from './components/queue-rack/queue-rack.component';
import { envConfigFactory, EnvconfigService } from './services/envconfig/envconfig.service';

const appRoutes: Routes = [
    {
        path: 'dashboard',
        component: DashboardComponent,
        data: { breadcrumb: 'Dashboard / Clusters' }
    },
    {
        path: 'clusters/:clusterName',
        component: ClusterContainerComponent,
        data: { breadcrumb: 'Clusters / :clusterName', prependRoot: true },
        children: [
            {
                path: 'info',
                component: ClusterInfoComponent,
                data: { breadcrumb: 'Info' }
            },
            {
                path: 'queues',
                component: QueuesViewComponent,
                data: { breadcrumb: 'Queues' }
            },
            {
                path: '',
                pathMatch: 'full',
                redirectTo: 'info'
            }
        ]
    },
    {
        path: '',
        pathMatch: 'full',
        redirectTo: 'dashboard'
    }
];

@NgModule({
    declarations: [
        AppComponent,
        DashboardComponent,
        ClusterInfoComponent,
        QueuesViewComponent,
        ClusterContainerComponent,
        DonutChartComponent,
        AreaChartComponent,
        JobStatusComponent,
        JobHistoryComponent,
        ContainerStatusComponent,
        ContainerHistoryComponent,
        QueueRackComponent
    ],
    imports: [
        BrowserModule,
        BrowserAnimationsModule,
        HttpClientModule,
        NgxSpinnerModule,
        MatCardModule,
        MatTabsModule,
        MatSelectModule,
        MatDividerModule,
        MatListModule,
        MatProgressBarModule,
        MatSidenavModule,
        RouterModule.forRoot(appRoutes)
    ],
    providers: [
        {
            provide: APP_INITIALIZER,
            useFactory: envConfigFactory,
            deps: [EnvconfigService],
            multi: true
        }
    ],
    bootstrap: [AppComponent]
})
export class AppModule {}
