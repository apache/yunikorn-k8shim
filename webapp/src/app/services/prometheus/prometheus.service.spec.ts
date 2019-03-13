import { TestBed, inject } from '@angular/core/testing';

import { PrometheusService } from './prometheus.service';

describe('PrometheusService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [PrometheusService]
    });
  });

  it('should be created', inject([PrometheusService], (service: PrometheusService) => {
    expect(service).toBeTruthy();
  }));
});
