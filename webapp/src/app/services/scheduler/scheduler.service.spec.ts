import { TestBed, inject } from '@angular/core/testing';

import { SchedulerService } from './scheduler.service';

describe('SchedulerService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SchedulerService]
    });
  });

  it('should be created', inject([SchedulerService], (service: SchedulerService) => {
    expect(service).toBeTruthy();
  }));
});
