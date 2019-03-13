import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
    providedIn: 'root'
})
export class PrometheusService {
    constructor(private httpClient: HttpClient) {}
}
