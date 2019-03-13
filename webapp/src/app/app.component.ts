import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
    isNavOpen = false;
    breadcrumbs: Array<Object> = [];

    constructor(private route: ActivatedRoute, private router: Router) {}

    ngOnInit() {
        this.router.events
            .pipe(filter(event => event instanceof NavigationEnd))
            .subscribe(event => {
                this.generateBreadcrumb();
            });
    }

    generateBreadcrumb() {
        this.breadcrumbs = [];
        let url = '';
        let currentRoute = this.route.root;
        do {
            const childrenRoutes = currentRoute.children;
            currentRoute = null;
            childrenRoutes.forEach(route => {
                if (route.outlet === 'primary') {
                    const routeSnapshot = route.snapshot;
                    if (routeSnapshot) {
                        url += '/' + routeSnapshot.url.map(segment => segment.path).join('/');
                        if (!!route.snapshot.data.breadcrumb) {
                            this.breadcrumbs.push({
                                label: route.snapshot.data.breadcrumb.includes(':')
                                    ? this.getResourceName(
                                          url,
                                          route.snapshot.data.breadcrumb,
                                          routeSnapshot.params,
                                          route.snapshot.data.breadcrumb.split(':')[1]
                                      )
                                    : route.snapshot.data.breadcrumb,
                                url: url
                            });
                            if (route.snapshot.data.prependRoot) {
                                this.breadcrumbs.unshift({
                                    label: 'Dashboard',
                                    url: '/'
                                });
                            }
                        }
                    }
                    currentRoute = route;
                }
            });
        } while (currentRoute);
    }

    getResourceName(url: string, label: string, params: Object, routeParam: string) {
        return label.replace(`:${routeParam}`, params[routeParam]);
    }

    toggleNavigation() {
        this.isNavOpen = !this.isNavOpen;
    }
}
