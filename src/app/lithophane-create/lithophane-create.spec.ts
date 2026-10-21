import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LithophaneCreate } from './lithophane-create';

describe('LithophaneCreate', () => {
  let component: LithophaneCreate;
  let fixture: ComponentFixture<LithophaneCreate>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LithophaneCreate]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LithophaneCreate);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
