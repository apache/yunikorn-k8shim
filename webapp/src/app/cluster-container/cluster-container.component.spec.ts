import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ClusterContainerComponent } from './cluster-container.component';

describe('ClusterContainerComponent', () => {
  let component: ClusterContainerComponent;
  let fixture: ComponentFixture<ClusterContainerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ClusterContainerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ClusterContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
