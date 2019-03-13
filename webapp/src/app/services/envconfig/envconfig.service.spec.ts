import { TestBed, inject } from '@angular/core/testing';

import { EnvconfigService } from './envconfig.service';

describe('EnvconfigService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [EnvconfigService]
    });
  });

  it('should be created', inject([EnvconfigService], (service: EnvconfigService) => {
    expect(service).toBeTruthy();
  }));
});
