import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ContainerHistoryComponent } from './container-history.component';

describe('ContainerHistoryComponent', () => {
  let component: ContainerHistoryComponent;
  let fixture: ComponentFixture<ContainerHistoryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ContainerHistoryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ContainerHistoryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
