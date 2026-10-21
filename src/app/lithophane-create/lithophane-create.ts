import { AfterViewInit, Component, ElementRef, OnDestroy, ViewChild, computed, effect, signal, untracked } from '@angular/core';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { DEFAULTS, Mesh, Params, buildMesh, prepare, swapSheet, toStl } from '../lithophane';

type NumKey = 'width' | 'arc' | 'hole' | 'baseH' | 'border' | 'minT' | 'maxT' | 'gamma' | 'pixel' | 'stripe' | 'layerH';
const SL: { k: NumKey; g: string; label: string; min: number; max: number; step: number; rgb?: boolean }[] = [
  { k: 'width', g: 'Shape', label: 'Picture width (mm)', min: 40, max: 300, step: 1 },
  { k: 'arc', g: 'Shape', label: 'Arc around the bulb (°)', min: 30, max: 350, step: 1 },
  { k: 'hole', g: 'Shape', label: 'Hole in base (mm)', min: 10, max: 80, step: 0.5 },
  { k: 'baseH', g: 'Shape', label: 'Base thickness (mm)', min: 1, max: 8, step: 0.5 },
  { k: 'border', g: 'Shape', label: 'Frame (mm)', min: 0, max: 8, step: 0.5 },
  { k: 'minT', g: 'Thickness', label: 'Thinnest = brightest (mm)', min: 0.4, max: 2, step: 0.1 },
  { k: 'maxT', g: 'Thickness', label: 'Thickest = darkest (mm)', min: 1.5, max: 5, step: 0.1 },
  { k: 'gamma', g: 'Thickness', label: 'Midtones (gamma)', min: 0.5, max: 2.5, step: 0.05 },
  { k: 'pixel', g: 'Print', label: 'Export detail (mm, lower = sharper)', min: 0.1, max: 0.4, step: 0.05 },
  { k: 'stripe', g: 'Print', label: 'Color band height (mm)', min: 0.4, max: 6, step: 0.2, rgb: true },
  { k: 'layerH', g: 'Print', label: 'Layer height (mm)', min: 0.08, max: 0.3, step: 0.04, rgb: true },
];

@Component({
  selector: 'app-lithophane-create',
  host: { class: 'flex h-screen bg-zinc-950 text-zinc-100 font-sans' },
  templateUrl: './lithophane-create.html',
  styleUrl: './lithophane-create.css',
})
export class LithophaneCreate implements AfterViewInit, OnDestroy {
  @ViewChild('cv') cv!: ElementRef<HTMLCanvasElement>;
  @ViewChild('host') host!: ElementRef<HTMLElement>;
  sliders = SL;
  groups = ['Shape', 'Thickness', 'Print'];
  p = signal<Params>({ ...DEFAULTS });
  img = signal<HTMLImageElement | null>(null);
  err = signal(''); busy = signal(false);
  inner = computed(() => 2 * (this.p().width + 2 * this.p().border) / (this.p().arc * Math.PI / 180));
  private r!: THREE.WebGLRenderer; private cam!: THREE.PerspectiveCamera; private ctl!: OrbitControls;
  private scene = new THREE.Scene(); private mesh?: THREE.Mesh; private bulb!: THREE.Mesh;
  private pending = 0; private raf = 0; private fit = true; private ro?: ResizeObserver;

  constructor() {
    effect(() => {
      this.p(); this.img();
      untracked(() => {   // at most one rebuild per frame, always with the latest values
        if (!this.pending) this.pending = requestAnimationFrame(() => { this.pending = 0; this.rebuild(); });
      });
    });
  }
  set<K extends keyof Params>(k: K, v: Params[K]) { this.p.update(o => ({ ...o, [k]: v })); }

  pick(e: Event) { this.load((e.target as HTMLInputElement).files?.[0]); }
  drop(e: DragEvent) { e.preventDefault(); this.load(e.dataTransfer?.files?.[0]); }
  private load(f?: File) {
    if (!f || !f.type.startsWith('image/')) return;
    const im = new Image(); im.onload = () => { this.fit = true; this.img.set(im); }; im.src = URL.createObjectURL(f);
  }

  ngAfterViewInit() {
    this.r = new THREE.WebGLRenderer({ canvas: this.cv.nativeElement, antialias: true, alpha: true });
    this.cam = new THREE.PerspectiveCamera(40, 1, 1, 5000);
    this.cam.position.set(0, 90, 260);
    this.ctl = new OrbitControls(this.cam, this.cv.nativeElement);
    this.bulb = new THREE.Mesh(new THREE.SphereGeometry(30, 32, 16),
      new THREE.MeshBasicMaterial({ color: 0xffe2a8, transparent: true, opacity: 0.9 }));
    this.scene.add(this.bulb);
    this.ro = new ResizeObserver(() => {
      const { clientWidth: w, clientHeight: h } = this.host.nativeElement;
      this.r.setSize(w, h, false); this.cam.aspect = w / h; this.cam.updateProjectionMatrix();
    });
    this.ro.observe(this.host.nativeElement);
    const loop = () => { this.ctl.update(); this.r.render(this.scene, this.cam); this.raf = requestAnimationFrame(loop); };
    loop();
  }
  ngOnDestroy() { cancelAnimationFrame(this.raf); cancelAnimationFrame(this.pending); this.ro?.disconnect(); this.r?.dispose(); }

  private rebuild() {
    const im = this.img(); if (!im) return;
    try {
      const p = { ...this.p(), pixel: Math.max(this.p().pixel, this.p().width / 240) };   // ~240 samples across = fast live preview
      const m = buildMesh(p, prepare(im, p));
      this.show(m, p); this.err.set('');
    } catch (e) { this.err.set((e as Error).message); }
  }
  private show(m: Mesh, p: Params) {
    const g = new THREE.BufferGeometry();
    g.setAttribute('position', new THREE.BufferAttribute(m.V, 3));
    g.setAttribute('color', new THREE.BufferAttribute(m.C, 3));
    g.setIndex(new THREE.BufferAttribute(m.F, 1));
    if (this.mesh) { this.scene.remove(this.mesh); this.mesh.geometry.dispose(); }
    this.mesh = new THREE.Mesh(g, new THREE.MeshBasicMaterial({ vertexColors: true, side: THREE.DoubleSide }));
    this.mesh.rotation.x = -Math.PI / 2;               // print Z-up -> viewer Y-up
    this.scene.add(this.mesh);
    this.bulb.position.set(0, p.baseH + 32, 0);
    if (this.fit) {
      this.ctl.target.set(0, m.height / 2, 0);
      this.cam.position.set(0, m.height * 0.9, m.outerR * 3.2); this.fit = false;
    }
  }

  async download() {
    const im = this.img(); if (!im) return;
    this.busy.set(true); await new Promise(r => setTimeout(r, 30));
    try {
      const p = this.p(), d = prepare(im, p), m = buildMesh(p, d);
      this.save(toStl(m), 'lithophane_lamp.stl');
      if (p.mode === 'rgb') this.save(new Blob([swapSheet(p, d)], { type: 'text/plain' }), 'lithophane_lamp_swaps.txt');
      this.err.set('');
    } catch (e) { this.err.set((e as Error).message); }
    this.busy.set(false);
  }
  private save(b: Blob, name: string) {
    const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = name; a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 2000);
  }
}