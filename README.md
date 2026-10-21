# Lithophane UI

Turn a picture into a 3D-printable **lithophane night lamp**, right in the browser.

**Live demo: https://mihailspaseski.github.io/lithophane-ui/**

Load an image, shape the curve with sliders, watch a live 3D preview of the lamp lit from inside, then download an STL. Nothing is uploaded anywhere: all the processing runs locally in your browser.

The curved picture stands on a base with a hole in it, so the lamp can slip over a light bulb socket and the picture sits around the bulb.

## Features

- **Live 3D preview**: rotate and zoom the lamp, lit from inside, with a 60 mm bulb shown so you can check the clearance. It updates in real time while you drag the sliders.
- **Curved panel**: set the picture width and the arc (30° to 350°) it wraps around the bulb. The inside diameter is shown live.
- **Base with a hole**: choose a **semicircle** or **full circle** base. The semicircle keeps a ring around the hole so it still grips the socket.
- **Two color modes**
  - **Single color**: one filament, the thickness makes the picture.
  - **RGB swaps**: prints in repeating red, green and blue bands, with a swap sheet (`.txt`) telling you at which layers to change filament.
- **Tone controls**: thinnest and thickest thickness, midtones (gamma), invert, and the frame around the picture.
- **STL export**: a binary STL with adjustable detail.
- Drag and drop a picture onto the preview.

## Getting started

You need a recent Node.js LTS and npm.

```bash
git clone https://github.com/mihailSpaseski/lithophane-ui.git
cd lithophane-ui
npm install
npm start
```

Then open http://localhost:4200.

If you are setting the project up from scratch, the extra packages are:

```bash
npm i three @types/three
```

Tailwind CSS is wired in through `src/styles.css` (`@import "tailwindcss";`).

## Deploying to GitHub Pages

The repo deploys itself. Every push to `main` runs `.github/workflows/deploy.yml`, which builds the app and publishes it to GitHub Pages.

One-time setup, after the repo is public:

1. Go to **Settings → Pages**.
2. Under **Build and deployment**, set **Source** to **GitHub Actions**.
3. Push to `main` (or run the workflow from the **Actions** tab). The live link above works after the first run finishes.

The workflow builds with `--base-href /lithophane-ui/`, because the site lives under the repo name. If you rename the repo, change that value in the workflow and the link at the top of this file.

If the build fails with a *budget exceeded* error (three.js makes the bundle larger than Angular's default limit), raise `maximumError` for the `initial` budget in `angular.json` to `3MB`.

## How to use it

1. Click **Choose a picture** (or drop one on the preview).
2. Pick a **color mode** and a **base shape**.
3. Adjust the sliders until the preview looks right. Make sure the **inside diameter** is bigger than your bulb. The card turns orange when a 60 mm bulb will not fit.
4. Set **Hole in base** to match your lamp's socket. Measure it, because sockets differ.
5. Click **Download STL**. In RGB mode you also get `lithophane_lamp_swaps.txt`.

## Printing tips

- Use an **LED bulb only**. PLA softens at around 55 to 60 °C, so a hot incandescent bulb would warp the lamp.
- Print the model **standing exactly as exported**, with the base flat on the bed. Do not rotate it.
- Use **100% infill**.
- For RGB mode, set the **layer height and first layer height** in your slicer to the same values as in the app, and add a pause or filament change before each layer listed in the swap sheet.
- Lower **Export detail** (for example 0.15 mm) gives a sharper picture but a much bigger file.
- Light the lamp from inside. The picture reads correctly when viewed from the outside.

## Project structure

```
src/
  main.ts
  app/
    app.routes.ts
    lithophane-create/
      lithophane-create.ts     Component: state (signals), three.js scene, export
      lithophane-create.html   Tailwind UI: sidebar controls and preview
      lithophane.ts            Engine: image to thickness map, mesh, STL, swap sheet
```

## How it works

1. The picture is resampled to a grid and converted to a **thickness map**: dark pixels become thick plastic and bright pixels become thin.
2. The grid is wrapped around a vertical cylinder to make the curved panel. The relief faces outward and the smooth side faces the bulb.
3. A base ring (full or half) with the socket hole is added, and the panel sinks slightly into it so the slicer merges them.
4. In RGB mode, each row of the picture uses only the red, green or blue channel of the band it is printed in, and the bands blend into the real colors when seen from a distance.
5. The preview uses a coarser mesh to stay fast. The export uses the full detail you chose.

This project is a browser version of an earlier Python script, `lithophane.py`.

## Not done yet

- **AMS / multi-material color** (cyan, magenta, yellow and white, one STL per filament) from the original script is not ported yet.

## Tech

Angular (standalone components and signals), Tailwind CSS, three.js.