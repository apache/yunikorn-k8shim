import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QueuesViewComponent } from './queues-view.component';

describe('QueuesViewComponent', () => {
  let component: QueuesViewComponent;
  let fixture: ComponentFixture<QueuesViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QueuesViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QueuesViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
