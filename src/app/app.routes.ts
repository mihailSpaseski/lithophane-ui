import { Routes } from '@angular/router';
import { LithophaneCreate } from './lithophane-create/lithophane-create';

export const routes: Routes = [
  { path: '', component: LithophaneCreate },
  { path: 'lithophane-create', component: LithophaneCreate },
];
