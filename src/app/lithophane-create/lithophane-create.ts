import { AfterViewInit, Component, ElementRef, OnDestroy, ViewChild, computed, effect, signal, untracked } from '@angular/core';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { Box, DEFAULTS, Mesh, Params, buildMesh, prepare, swapSheet, toStl } from '../lithophane';

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

// Build volumes in mm (X x Y x Z). Check Bambu Studio if you rely on the exact edge.
export const PRINTERS = [
  { id: 'a1mini', name: 'A1 mini', x: 180, y: 180, z: 180 },
  { id: 'a1', name: 'A1', x: 256, y: 256, z: 256 },
  { id: 'p1p', name: 'P1P', x: 256, y: 256, z: 256 },
  { id: 'p1s', name: 'P1S', x: 256, y: 256, z: 256 },
  { id: 'p2s', name: 'P2S', x: 256, y: 256, z: 256 },
  { id: 'x1c', name: 'X1 Carbon', x: 256, y: 256, z: 256 },
  { id: 'x1e', name: 'X1E', x: 256, y: 256, z: 256 },
  { id: 'x2d', name: 'X2D', x: 256, y: 256, z: 260 },
  { id: 'h2s', name: 'H2S', x: 340, y: 320, z: 340 },
  { id: 'h2d', name: 'H2D', x: 350, y: 320, z: 325 },
  { id: 'h2c', name: 'H2C', x: 330, y: 320, z: 325 },
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
  printers = PRINTERS;
  printer = signal<string | null>(null);
  dims = signal<{ x: number; y: number; z: number } | null>(null);
  fitInfo = computed(() => {
    const d = this.dims(), pr = PRINTERS.find(x => x.id === this.printer());
    if (!d || !pr) return null;
    const over = [d.x > pr.x ? 'width' : '', d.y > pr.y ? 'depth' : '', d.z > pr.z ? 'height' : ''].filter(Boolean);
    return { pr, ok: !over.length, over };
  });
  p = signal<Params>({ ...DEFAULTS });
  img = signal<HTMLImageElement | null>(null);
  err = signal(''); busy = signal(false);
  inner = computed(() => 2 * (this.p().width + 2 * this.p().border) / (this.p().arc * Math.PI / 180));
  private r!: THREE.WebGLRenderer; private cam!: THREE.PerspectiveCamera; private ctl!: OrbitControls;
  private scene = new THREE.Scene(); private mesh?: THREE.Mesh; private bulb!: THREE.Mesh;
  private world = new THREE.Group(); private rulers = new THREE.Group(); private bed = new THREE.Group();
  private pending = 0; private raf = 0; private fit = true; private ro?: ResizeObserver;

  constructor() {
    effect(() => {
      this.p(); this.img(); this.printer();
      untracked(() => {   // at most one rebuild per frame, always with the latest values
        if (!this.pending) this.pending = requestAnimationFrame(() => { this.pending = 0; this.rebuild(); });
      });
    });
  }
  choose(id: string) { this.fit = true; this.printer.set(this.printer() === id ? null : id); }
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
    this.world.rotation.x = -Math.PI / 2;               // print Z-up -> viewer Y-up
    this.world.add(this.bulb, this.rulers, this.bed);
    this.scene.add(this.world);
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
    if (this.mesh) { this.world.remove(this.mesh); this.mesh.geometry.dispose(); (this.mesh.material as THREE.Material).dispose(); }
    this.mesh = new THREE.Mesh(g, new THREE.MeshBasicMaterial({ vertexColors: true, side: THREE.DoubleSide }));
    this.world.add(this.mesh);

    const b = m.box, dx = b.x1 - b.x0, dy = b.y1 - b.y0, dz = b.z1;
    const cx = (b.x0 + b.x1) / 2, cy = (b.y0 + b.y1) / 2;
    const pr = PRINTERS.find(x => x.id === this.printer());
    this.dims.set({ x: dx, y: dy, z: dz });
    this.bulb.position.set(0, 0, p.baseH + 32);
    this.drawRulers(b);
    this.drawBed(pr, cx, cy);
    if (this.fit) {
      const size = Math.max(dx, dy, dz, pr ? Math.max(pr.x, pr.y, pr.z) * 0.95 : 0);
      this.ctl.target.set(cx, dz / 2, -cy);
      this.cam.position.set(cx, dz / 2 + size * 0.6, -cy + size * 1.7); this.fit = false;
    }
  }

  // ---- rulers around the model (X, Y, Z) with a tick every 10 mm
  private drawRulers(b: Box) {
    this.clear(this.rulers);
    const off = 12, z = 0.2, seg: number[] = [];
    const line = (a: number[], c: number[]) => seg.push(...a, ...c);
    const add = (a: number[], c: number[], tick: number[]) => {
      line(a, c);
      const len = Math.hypot(c[0] - a[0], c[1] - a[1], c[2] - a[2]), u = [0, 1, 2].map(i => (c[i] - a[i]) / len);
      for (let t = 0; t <= len + 0.01; t += 10) {
        const q = [0, 1, 2].map(i => a[i] + u[i] * t); line(q, [q[0] + tick[0], q[1] + tick[1], q[2] + tick[2]]);
      }
      line(a, [a[0] + tick[0] * 2.5, a[1] + tick[1] * 2.5, a[2] + tick[2] * 2.5]);
      line(c, [c[0] + tick[0] * 2.5, c[1] + tick[1] * 2.5, c[2] + tick[2] * 2.5]);
    };
    const { x0, x1, y0, y1, z1 } = b;
    add([x0, y0 - off, z], [x1, y0 - off, z], [0, -2, 0]);           // width, in front
    add([x1 + off, y0, z], [x1 + off, y1, z], [2, 0, 0]);            // depth, on the right
    add([x0 - off, y0 - off, 0], [x0 - off, y0 - off, z1], [-2, 0, 0]); // height, front-left
    line([x0, y0, z], [x0, y0 - off, z]); line([x1, y0, z], [x1, y0 - off, z]);          // extension lines
    line([x1, y0, z], [x1 + off, y0, z]); line([x1, y1, z], [x1 + off, y1, z]);
    line([x0, y0, z1], [x0 - off, y0 - off, z1]);
    this.rulers.add(this.lines(seg, 0xfbbf24, 1));
    const h = Math.max(5, Math.max(x1 - x0, y1 - y0, z1) / 16);
    this.rulers.add(
      this.label(`${(x1 - x0).toFixed(1)} mm`, [(x0 + x1) / 2, y0 - off - h, z], h),
      this.label(`${(y1 - y0).toFixed(1)} mm`, [x1 + off + h * 2, (y0 + y1) / 2, z], h),
      this.label(`${z1.toFixed(1)} mm`, [x0 - off - h * 2, y0 - off, z1 / 2], h));
  }

  // ---- printer bed, build volume and a 10 mm grid, centred under the model
  private drawBed(pr: (typeof PRINTERS)[number] | undefined, cx: number, cy: number) {
    this.clear(this.bed);
    if (!pr) return;
    const { x: bx, y: by, z: bz } = pr;
    this.bed.position.set(cx, cy, 0);
    const plate = new THREE.Mesh(new THREE.PlaneGeometry(bx, by),
      new THREE.MeshBasicMaterial({ color: 0x2a2a31, transparent: true, opacity: 0.8, side: THREE.DoubleSide, depthWrite: false }));
    plate.position.z = -0.3; this.bed.add(plate);
    const fine: number[] = [], bold: number[] = [];
    for (let i = 0; i <= bx; i += 10) (i % 50 ? fine : bold).push(-bx / 2 + i, -by / 2, -0.2, -bx / 2 + i, by / 2, -0.2);
    for (let i = 0; i <= by; i += 10) (i % 50 ? fine : bold).push(-bx / 2, -by / 2 + i, -0.2, bx / 2, -by / 2 + i, -0.2);
    bold.push(-bx / 2, -by / 2, -0.2, bx / 2, -by / 2, -0.2, bx / 2, -by / 2, -0.2, bx / 2, by / 2, -0.2,
      bx / 2, by / 2, -0.2, -bx / 2, by / 2, -0.2, -bx / 2, by / 2, -0.2, -bx / 2, -by / 2, -0.2);
    this.bed.add(this.lines(fine, 0x3a3a44, 1), this.lines(bold, 0x8a8a96, 1));
    const vol = new THREE.LineSegments(new THREE.EdgesGeometry(new THREE.BoxGeometry(bx, by, bz)),
      new THREE.LineBasicMaterial({ color: 0x6b7280, transparent: true, opacity: 0.5 }));
    vol.position.z = bz / 2; this.bed.add(vol);
    const h = Math.max(6, bx / 38);
    for (let k = 0; k * 50 <= Math.max(bx, by, bz); k++) {          // rulers every 50 mm from the bed corner
      const v = k * 50;
      if (v <= bx) this.bed.add(this.label(`${v}`, [-bx / 2 + v, -by / 2 - h, 0], h, '#a1a1aa'));
      if (v <= by) this.bed.add(this.label(`${v}`, [-bx / 2 - h * 1.3, -by / 2 + v, 0], h, '#a1a1aa'));
      if (v > 0 && v <= bz) this.bed.add(this.label(`${v}`, [-bx / 2 - h * 1.3, -by / 2, v], h, '#a1a1aa'));
    }
    this.bed.add(this.label(`${pr.name}  ${bx} × ${by} × ${bz} mm`, [0, -by / 2, bz + h], h * 1.2, '#e4e4e7'));
  }

  private lines(pos: number[], color: number, opacity: number) {
    const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
    return new THREE.LineSegments(g, new THREE.LineBasicMaterial({ color, transparent: opacity < 1, opacity }));
  }
  private label(text: string, pos: number[], h: number, color = '#fbbf24') {
    const cv = document.createElement('canvas'); cv.width = 384; cv.height = 64;
    const c = cv.getContext('2d')!;
    c.font = '600 34px system-ui, sans-serif'; c.textAlign = 'center'; c.textBaseline = 'middle';
    c.fillStyle = color; c.fillText(text, 192, 32);
    const s = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(cv), depthTest: false, transparent: true }));
    s.position.set(pos[0], pos[1], pos[2]); s.scale.set(h * 6, h, 1); return s;
  }
  private clear(g: THREE.Group) {
    g.children.slice().forEach(o => {
      g.remove(o); const a = o as any;
      a.geometry?.dispose(); a.material?.map?.dispose(); a.material?.dispose();
    });
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