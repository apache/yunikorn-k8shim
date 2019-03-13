import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QueueRackComponent } from './queue-rack.component';

describe('QueueRackComponent', () => {
  let component: QueueRackComponent;
  let fixture: ComponentFixture<QueueRackComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QueueRackComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QueueRackComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
