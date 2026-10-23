var qw=Object.defineProperty,Xw=Object.defineProperties;var Yw=Object.getOwnPropertyDescriptors;var k_=Object.getOwnPropertySymbols;var Zw=Object.prototype.hasOwnProperty,Kw=Object.prototype.propertyIsEnumerable;var B_=(n,e,t)=>e in n?qw(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,oe=(n,e)=>{for(var t in e||={})Zw.call(e,t)&&B_(n,t,e[t]);if(k_)for(var t of k_(e))Kw.call(e,t)&&B_(n,t,e[t]);return n},tt=(n,e)=>Xw(n,Yw(e));var vn=null,Yl=!1,Fp=1,Jw=null,Cn=Symbol("SIGNAL");function De(n){let e=vn;return vn=n,e}function Ql(){return vn}var gs={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function lo(n){if(Yl)throw new Error("");if(vn===null)return;vn.consumerOnSignalRead(n);let e=vn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=vn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:vn.producers,t!==void 0&&t.producer===n)){vn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===vn&&(!i||eT(r,vn)))return;let s=fo(vn),o={producer:n,consumer:vn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};vn.producersTail=o,e!==void 0?e.nextProducer=o:vn.producers=o,s&&G_(n,o)}function V_(){Fp++}function eu(n){if(!(fo(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Fp)){if(!n.producerMustRecompute(n)&&!Ba(n)){Jl(n);return}n.producerRecomputeValue(n),Jl(n)}}function Up(n){if(n.consumers===void 0)return;let e=Yl;Yl=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||Qw(i)}}finally{Yl=e}}function kp(){return vn?.consumerAllowSignalWrites!==!1}function Qw(n){n.dirty=!0,Up(n),n.consumerMarkedDirty?.(n)}function Jl(n){n.dirty=!1,n.lastCleanEpoch=Fp}function vs(n){return n&&H_(n),De(n)}function H_(n){n.producersTail=void 0,n.recomputing=!0}function uo(n,e){De(e),n&&z_(n)}function z_(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(fo(n))do t=Bp(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Ba(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(eu(t),i!==t.version))return!0}return!1}function ys(n){if(fo(n)){let e=n.producers;for(;e!==void 0;)e=Bp(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function G_(n,e){let t=n.consumersTail,i=fo(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)G_(r.producer,r)}function Bp(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!fo(e)){let s=e.producers;for(;s!==void 0;)s=Bp(s)}return t}function fo(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function tu(n){Jw?.(n)}function eT(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function nu(n,e){return Object.is(n,e)}function iu(n,e){let t=Object.create(tT);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(eu(t),lo(t),t.value===ka)throw t.error;return t.value};return i[Cn]=t,tu(t),i}var Zl=Symbol("UNSET"),Kl=Symbol("COMPUTING"),ka=Symbol("ERRORED"),tT=tt(oe({},gs),{value:Zl,dirty:!0,error:null,equal:nu,kind:"computed",producerMustRecompute(n){return n.value===Zl||n.value===Kl},producerRecomputeValue(n){if(n.value===Kl)throw new Error("");let e=n.value;n.value=Kl;let t=vs(n),i,r=!1;try{i=n.computation(),De(null),r=e!==Zl&&e!==ka&&i!==ka&&n.equal(e,i)}catch(s){i=ka,n.error=s}finally{uo(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function nT(){throw new Error}var W_=nT;function j_(n){W_(n)}function Vp(n){W_=n}var iT=null;function Hp(n,e){let t=Object.create(ru);t.value=n,e!==void 0&&(t.equal=e);let i=()=>$_(t);return i[Cn]=t,tu(t),[i,o=>ho(t,o),o=>zp(t,o)]}function $_(n){return lo(n),n.value}function ho(n,e){kp()||j_(n),n.equal(n.value,e)||(n.value=e,rT(n))}function zp(n,e){kp()||j_(n),ho(n,e(n.value))}var ru=tt(oe({},gs),{equal:nu,value:void 0,kind:"signal"});function rT(n){n.version++,V_(),Up(n),iT?.(n)}var Gp=tt(oe({},gs),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Wp(n){if(n.dirty=!1,n.version>0&&!Ba(n))return;n.version++;let e=vs(n);try{n.cleanup(),n.fn()}finally{uo(n,e)}}function ze(n){return typeof n=="function"}function po(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var su=po(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Va(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var on=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(ze(i))try{i()}catch(s){e=s instanceof su?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{q_(s)}catch(o){e=e??[],o instanceof su?e=[...e,...o.errors]:e.push(o)}}if(e)throw new su(e)}}add(e){var t;if(e&&e!==this)if(this.closed)q_(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Va(t,e)}remove(e){let{_finalizers:t}=this;t&&Va(t,e),e instanceof n&&e._removeParent(this)}};on.EMPTY=(()=>{let n=new on;return n.closed=!0,n})();var jp=on.EMPTY;function ou(n){return n instanceof on||n&&"closed"in n&&ze(n.remove)&&ze(n.add)&&ze(n.unsubscribe)}function q_(n){ze(n)?n():n.unsubscribe()}var hi={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var mo={setTimeout(n,e,...t){let{delegate:i}=mo;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=mo;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function au(n){mo.setTimeout(()=>{let{onUnhandledError:e}=hi;if(e)e(n);else throw n})}function Ha(){}var X_=$p("C",void 0,void 0);function Y_(n){return $p("E",void 0,n)}function Z_(n){return $p("N",n,void 0)}function $p(n,e,t){return{kind:n,value:e,error:t}}var _s=null;function go(n){if(hi.useDeprecatedSynchronousErrorHandling){let e=!_s;if(e&&(_s={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=_s;if(_s=null,t)throw i}}else n()}function K_(n){hi.useDeprecatedSynchronousErrorHandling&&_s&&(_s.errorThrown=!0,_s.error=n)}var xs=class extends on{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,ou(e)&&e.add(this)):this.destination=aT}static create(e,t,i){return new vo(e,t,i)}next(e){this.isStopped?Xp(Z_(e),this):this._next(e)}error(e){this.isStopped?Xp(Y_(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Xp(X_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},sT=Function.prototype.bind;function qp(n,e){return sT.call(n,e)}var Yp=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){cu(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){cu(i)}else cu(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){cu(t)}}},vo=class extends xs{constructor(e,t,i){super();let r;if(ze(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&hi.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&qp(e.next,s),error:e.error&&qp(e.error,s),complete:e.complete&&qp(e.complete,s)}):r=e}this.destination=new Yp(r)}};function cu(n){hi.useDeprecatedSynchronousErrorHandling?K_(n):au(n)}function oT(n){throw n}function Xp(n,e){let{onStoppedNotification:t}=hi;t&&mo.setTimeout(()=>t(n,e))}var aT={closed:!0,next:Ha,error:oT,complete:Ha};var yo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function pi(n){return n}function Zp(...n){return Kp(n)}function Kp(n){return n.length===0?pi:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var ft=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=lT(t)?t:new vo(t,i,r);return go(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=J_(i),new i((r,s)=>{let o=new vo({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[yo](){return this}pipe(...t){return Kp(t)(this)}toPromise(t){return t=J_(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function J_(n){var e;return(e=n??hi.Promise)!==null&&e!==void 0?e:Promise}function cT(n){return n&&ze(n.next)&&ze(n.error)&&ze(n.complete)}function lT(n){return n&&n instanceof xs||cT(n)&&ou(n)}function uT(n){return ze(n?.lift)}function vt(n){return e=>{if(uT(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Et(n,e,t,i,r){return new Jp(n,e,t,i,r)}var Jp=class extends xs{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var Q_=po(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var an=(()=>{class n extends ft{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new lu(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Q_}next(t){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?jp:(this.currentObservers=null,s.push(t),new on(()=>{this.currentObservers=null,Va(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new ft;return t.source=this,t}}return n.create=(e,t)=>new lu(e,t),n})(),lu=class extends an{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:jp}};var cn=class extends an{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var ln=new ft(n=>n.complete());function ex(n){return n&&ze(n.schedule)}function tx(n){return n[n.length-1]}function nx(n){return ze(tx(n))?n.pop():void 0}function Pr(n){return ex(tx(n))?n.pop():void 0}function rx(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function ix(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ss(n){return this instanceof Ss?(this.v=n,this):new Ss(n)}function sx(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(m){return Promise.resolve(m).then(h,d)}}function a(h,m){i[h]&&(r[h]=function(y){return new Promise(function(g,p){s.push([h,y,g,p])>1||c(h,y)})},m&&(r[h]=m(r[h])))}function c(h,m){try{l(i[h](m))}catch(y){f(s[0][3],y)}}function l(h){h.value instanceof Ss?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,m){h(m),s.shift(),s.length&&c(s[0][0],s[0][1])}}function ox(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof ix=="function"?ix(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var uu=n=>n&&typeof n.length=="number"&&typeof n!="function";function du(n){return ze(n?.then)}function fu(n){return ze(n[yo])}function hu(n){return Symbol.asyncIterator&&ze(n?.[Symbol.asyncIterator])}function pu(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function dT(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var mu=dT();function gu(n){return ze(n?.[mu])}function vu(n){return sx(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Ss(t.read());if(r)return yield Ss(void 0);yield yield Ss(i)}}finally{t.releaseLock()}})}function yu(n){return ze(n?.getReader)}function tn(n){if(n instanceof ft)return n;if(n!=null){if(fu(n))return fT(n);if(uu(n))return hT(n);if(du(n))return pT(n);if(hu(n))return ax(n);if(gu(n))return mT(n);if(yu(n))return gT(n)}throw pu(n)}function fT(n){return new ft(e=>{let t=n[yo]();if(ze(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function hT(n){return new ft(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function pT(n){return new ft(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,au)})}function mT(n){return new ft(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function ax(n){return new ft(e=>{vT(n,e).catch(t=>e.error(t))})}function gT(n){return ax(vu(n))}function vT(n,e){var t,i,r,s;return rx(this,void 0,void 0,function*(){try{for(t=ox(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function On(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function _u(n,e=0){return vt((t,i)=>{t.subscribe(Et(i,r=>On(i,n,()=>i.next(r),e),()=>On(i,n,()=>i.complete(),e),r=>On(i,n,()=>i.error(r),e)))})}function xu(n,e=0){return vt((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function cx(n,e){return tn(n).pipe(xu(e),_u(e))}function lx(n,e){return tn(n).pipe(xu(e),_u(e))}function ux(n,e){return new ft(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function dx(n,e){return new ft(t=>{let i;return On(t,e,()=>{i=n[mu](),On(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>ze(i?.return)&&i.return()})}function Su(n,e){if(!n)throw new Error("Iterable cannot be null");return new ft(t=>{On(t,e,()=>{let i=n[Symbol.asyncIterator]();On(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function fx(n,e){return Su(vu(n),e)}function hx(n,e){if(n!=null){if(fu(n))return cx(n,e);if(uu(n))return ux(n,e);if(du(n))return lx(n,e);if(hu(n))return Su(n,e);if(gu(n))return dx(n,e);if(yu(n))return fx(n,e)}throw pu(n)}function Gt(n,e){return e?hx(n,e):tn(n)}function nt(...n){let e=Pr(n);return Gt(n,e)}function Qp(n,e){let t=ze(n)?n:()=>n,i=r=>r.error(t());return new ft(e?r=>e.schedule(i,0,r):i)}function bu(n){return!!n&&(n instanceof ft||ze(n.lift)&&ze(n.subscribe))}var bs=po(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function Ft(n,e){return vt((t,i)=>{let r=0;t.subscribe(Et(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:yT}=Array;function _T(n,e){return yT(e)?n(...e):n(e)}function px(n){return Ft(e=>_T(n,e))}var{isArray:xT}=Array,{getPrototypeOf:ST,prototype:bT,keys:MT}=Object;function mx(n){if(n.length===1){let e=n[0];if(xT(e))return{args:e,keys:null};if(ET(e)){let t=MT(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function ET(n){return n&&typeof n=="object"&&ST(n)===bT}function gx(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function em(...n){let e=Pr(n),t=nx(n),{args:i,keys:r}=mx(n);if(i.length===0)return Gt([],e);let s=new ft(wT(i,e,r?o=>gx(r,o):pi));return t?s.pipe(px(t)):s}function wT(n,e,t=pi){return i=>{vx(e,()=>{let{length:r}=n,s=new Array(r),o=r,a=r;for(let c=0;c<r;c++)vx(e,()=>{let l=Gt(n[c],e),u=!1;l.subscribe(Et(i,d=>{s[c]=d,u||(u=!0,a--),a||i.next(t(s.slice()))},()=>{--o||i.complete()}))},i)},i)}}function vx(n,e,t){n?On(t,n,e):e()}function yx(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=y=>l<i?m(y):c.push(y),m=y=>{s&&e.next(y),l++;let g=!1;tn(t(y,u++)).subscribe(Et(e,p=>{r?.(p),s?h(p):e.next(p)},()=>{g=!0},void 0,()=>{if(g)try{for(l--;c.length&&l<i;){let p=c.shift();o?On(e,o,()=>m(p)):m(p)}f()}catch(p){e.error(p)}}))};return n.subscribe(Et(e,h,()=>{d=!0,f()})),()=>{a?.()}}function In(n,e,t=1/0){return ze(e)?In((i,r)=>Ft((s,o)=>e(i,s,r,o))(tn(n(i,r))),t):(typeof e=="number"&&(t=e),vt((i,r)=>yx(i,r,n,t)))}function _x(n=1/0){return In(pi,n)}function xx(){return _x(1)}function _o(...n){return xx()(Gt(n,Pr(n)))}function za(n){return new ft(e=>{tn(n()).subscribe(e)})}function or(n,e){return vt((t,i)=>{let r=0;t.subscribe(Et(i,s=>n.call(e,s,r++)&&i.next(s)))})}function Ga(n){return vt((e,t)=>{let i=null,r=!1,s;i=e.subscribe(Et(t,void 0,void 0,o=>{s=tn(n(o,Ga(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function Mu(n,e){return ze(e)?In(n,e,1):In(n,1)}function Sx(n){return vt((e,t)=>{let i=!1;e.subscribe(Et(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function ar(n){return n<=0?()=>ln:vt((e,t)=>{let i=0;e.subscribe(Et(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function bx(n=TT){return vt((e,t)=>{let i=!1;e.subscribe(Et(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function TT(){return new bs}function tm(n){return vt((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function cr(n,e){let t=arguments.length>=2;return i=>i.pipe(n?or((r,s)=>n(r,s,i)):pi,ar(1),t?Sx(e):bx(()=>new bs))}function Eu(n){return n<=0?()=>ln:vt((e,t)=>{let i=[];e.subscribe(Et(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function nm(...n){let e=Pr(n);return vt((t,i)=>{(e?_o(n,t,e):_o(n,t)).subscribe(i)})}function mi(n,e){return vt((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(Et(i,c=>{r?.unsubscribe();let l=0,u=s++;tn(n(c,u)).subscribe(r=Et(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}function Wa(n){return vt((e,t)=>{tn(n).subscribe(Et(t,()=>t.complete(),Ha)),!t.closed&&e.subscribe(t)})}function ni(n,e,t){let i=ze(n)||e||t?{next:n,error:e,complete:t}:n;return i?vt((r,s)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let a=!0;r.subscribe(Et(s,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),s.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),s.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),s.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):pi}var im;function wu(){return im}function Vi(n){let e=im;return im=n,e}var Mx=Symbol("NotFound");function xo(n){return n===Mx||n?.name==="\u0275NotFound"}function Ex(n){let e=De(null);try{return n()}finally{De(e)}}var gm="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Ae=class extends Error{code;constructor(e,t){super(Ka(e,t)),this.code=e}};function CT(n){return`NG0${Math.abs(n)}`}function Ka(n,e){return`${CT(n)}${e?": "+e:""}`}function yt(n){for(let e in n)if(n[e]===yt)return e;throw Error("")}function Pu(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Pu).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Lu(n,e){return n?e?`${n} ${e}`:n:e||""}var IT=yt({__forward_ref__:yt});function Ou(n){return n.__forward_ref__=Ou,n}function Fn(n){return vm(n)?n():n}function vm(n){return typeof n=="function"&&n.hasOwnProperty(IT)&&n.__forward_ref__===Ou}function Ue(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Ja(n){return AT(n,Fu)}function ym(n){return Ja(n)!==null}function AT(n,e){return n.hasOwnProperty(e)&&n[e]||null}function DT(n){let e=n?.[Fu]??null;return e||null}function sm(n){return n&&n.hasOwnProperty(Cu)?n[Cu]:null}var Fu=yt({\u0275prov:yt}),Cu=yt({\u0275inj:yt}),Oe=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Ue({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function _m(n){return n&&!!n.\u0275providers}var xm=yt({\u0275cmp:yt}),Sm=yt({\u0275dir:yt}),bm=yt({\u0275pipe:yt}),Mm=yt({\u0275mod:yt}),$a=yt({\u0275fac:yt}),Is=yt({__NG_ELEMENT_ID__:yt}),wx=yt({__NG_ENV_ID__:yt});function Em(n){return Uu(n,"@NgModule"),n[Mm]||null}function Fr(n){return Uu(n,"@Component"),n[xm]||null}function wm(n){return Uu(n,"@Directive"),n[Sm]||null}function Ax(n){return Uu(n,"@Pipe"),n[bm]||null}function Uu(n,e){if(n==null)throw new Ae(-919,!1)}function Hn(n){return typeof n=="string"?n:n==null?"":String(n)}var Dx=yt({ngErrorCode:yt}),RT=yt({ngErrorMessage:yt}),NT=yt({ngTokenPath:yt});function Tm(n,e){return Rx("",-200,e)}function ku(n,e){throw new Ae(-201,!1)}function Rx(n,e,t){let i=new Ae(e,n);return i[Dx]=e,i[RT]=n,t&&(i[NT]=t),i}function PT(n){return n[Dx]}var om;function Nx(){return om}function Bn(n){let e=om;return om=n,e}function Cm(n,e,t){let i=Ja(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;ku(n,"")}var LT={},Ms=LT,OT="__NG_DI_FLAG__",am=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Es(t)||0;try{return this.injector.get(e,i&8?null:Ms,i)}catch(r){if(xo(r))return r;throw r}}};function FT(n,e=0){let t=wu();if(t===void 0)throw new Ae(-203,!1);if(t===null)return Cm(n,void 0,e);{let i=UT(e),r=t.retrieve(n,i);if(xo(r)){if(i.optional)return null;throw r}return r}}function Xe(n,e=0){return(Nx()||FT)(Fn(n),e)}function re(n,e){return Xe(n,Es(e))}function Es(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function UT(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function cm(n){let e=[];for(let t=0;t<n.length;t++){let i=Fn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Ae(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=kT(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(Xe(r,s))}else e.push(Xe(i))}return e}function kT(n){return n[OT]}function ws(n,e){let t=n.hasOwnProperty($a);return t?n[$a]:null}function Px(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Lx(n){return n.flat(Number.POSITIVE_INFINITY)}function Bu(n,e){n.forEach(t=>Array.isArray(t)?Bu(t,e):e(t))}function Im(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Qa(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Ox(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function Vu(n,e,t){let i=bo(n,e);return i>=0?n[i|1]=t:(i=~i,Ox(n,i,e,t)),i}function Hu(n,e){let t=bo(n,e);if(t>=0)return n[t|1]}function bo(n,e){return BT(n,e,1)}function BT(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var As={},Hi=[],Ds=new Oe(""),Am=new Oe("",-1),Dm=new Oe(""),qa=class{get(e,t=Ms){if(t===Ms){let r=Rx("",-201);throw r.name="\u0275NotFound",r}return t}};function Mo(n){return{\u0275providers:n}}function Fx(n){return Mo([{provide:Ds,multi:!0,useValue:n}])}function Ux(...n){return{\u0275providers:Rm(!0,n),\u0275fromNgModule:!0}}function Rm(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Bu(e,o=>{let a=o;Iu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&kx(r,s),t}function kx(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Nm(r,s=>{e(s,i)})}}function Iu(n,e,t,i){if(n=Fn(n),!n)return!1;let r=null,s=sm(n),o=!s&&Fr(n);if(!s&&!o){let c=n.ngModule;if(s=sm(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Iu(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Bu(s.imports,u=>{Iu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&kx(l,e)}if(!a){let l=ws(r)||(()=>new r);e({provide:r,useFactory:l,deps:Hi},r),e({provide:Dm,useValue:r,multi:!0},r),e({provide:Ds,useValue:()=>Xe(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;Nm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Nm(n,e){for(let t of n)_m(t)&&(t=t.\u0275providers),Array.isArray(t)?Nm(t,e):e(t)}var VT=yt({provide:String,useValue:yt});function Bx(n){return n!==null&&typeof n=="object"&&VT in n}function HT(n){return!!(n&&n.useExisting)}function zT(n){return!!(n&&n.useFactory)}function Au(n){return typeof n=="function"}var ec=new Oe(""),Tu={},Tx={},rm;function tc(){return rm===void 0&&(rm=new qa),rm}var qt=class{},Ts=class extends qt{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,um(e,o=>this.processProvider(o)),this.records.set(Am,So(void 0,this)),r.has("environment")&&this.records.set(qt,So(void 0,this));let s=this.records.get(ec);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Dm,Hi,{self:!0}))}retrieve(e,t){let i=Es(t)||0;try{return this.get(e,Ms,i)}catch(r){if(xo(r))return r;throw r}}destroy(){ja(this),this._destroyed=!0;let e=De(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),De(e)}}onDestroy(e){return ja(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){ja(this);let t=Vi(this),i=Bn(void 0),r;try{return e()}finally{Vi(t),Bn(i)}}get(e,t=Ms,i){if(ja(this),e.hasOwnProperty(wx))return e[wx](this);let r=Es(i),s,o=Vi(this),a=Bn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=qT(e)&&Ja(e);u&&this.injectableDefInScope(u)?l=So(lm(e),Tu):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?tc():this.parent;return t=r&8&&t===Ms?null:t,c.get(e,t)}catch(c){let l=PT(c);throw l===-200||l===-201?new Ae(l,null):c}finally{Bn(a),Vi(o)}}resolveInjectorInitializers(){let e=De(null),t=Vi(this),i=Bn(void 0),r;try{let s=this.get(Ds,Hi,{self:!0});for(let o of s)o()}finally{Vi(t),Bn(i),De(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=Fn(e);let t=Au(e)?e:Fn(e&&e.provide),i=WT(e);if(!Au(e)&&e.multi===!0){let r=this.records.get(t);r||(r=So(void 0,Tu,!0),r.factory=()=>cm(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=De(null);try{if(t.value===Tx)throw Tm("");return t.value===Tu&&(t.value=Tx,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&$T(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{De(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Fn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function lm(n){let e=Ja(n),t=e!==null?e.factory:ws(n);if(t!==null)return t;if(n instanceof Oe)throw new Ae(-204,!1);if(n instanceof Function)return GT(n);throw new Ae(-204,!1)}function GT(n){if(n.length>0)throw new Ae(-204,!1);let t=DT(n);return t!==null?()=>t.factory(n):()=>new n}function WT(n){if(Bx(n))return So(void 0,n.useValue);{let e=Vx(n);return So(e,Tu)}}function Vx(n,e,t){let i;if(Au(n)){let r=Fn(n);return ws(r)||lm(r)}else if(Bx(n))i=()=>Fn(n.useValue);else if(zT(n))i=()=>n.useFactory(...cm(n.deps||[]));else if(HT(n))i=(r,s)=>Xe(Fn(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=Fn(n&&(n.useClass||n.provide));if(jT(n))i=()=>new r(...cm(n.deps));else return ws(r)||lm(r)}return i}function ja(n){if(n.destroyed)throw new Ae(-205,!1)}function So(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function jT(n){return!!n.deps}function $T(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function qT(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function um(n,e){for(let t of n)Array.isArray(t)?um(t,e):t&&_m(t)?um(t.\u0275providers,e):e(t)}function yn(n,e){let t;n instanceof Ts?(ja(n),t=n):t=new am(n);let i,r=Vi(t),s=Bn(void 0);try{return e()}finally{Vi(r),Bn(s)}}function Hx(){return Nx()!==void 0||wu()!=null}var vi=0,Fe=1,Ge=2,Xt=3,ii=4,ri=5,Eo=6,wo=7,Vt=8,dr=9,zi=10,Yt=11,To=12,Pm=13,Rs=14,zn=15,Ur=16,Ns=17,Gi=18,fr=19,Lm=20,lr=21,zu=22,Lr=23,Gn=24,Gu=25,kr=26,un=27,zx=1,Om=6,Br=7,nc=8,Ps=9,Ut=10;function Vr(n){return Array.isArray(n)&&typeof n[zx]=="object"}function yi(n){return Array.isArray(n)&&n[zx]===!0}function Fm(n){return(n.flags&4)!==0}function Hr(n){return n.componentOffset>-1}function Wu(n){return(n.flags&1)===1}function Ls(n){return!!n.template}function Co(n){return(n[Ge]&512)!==0}function Os(n){return(n[Ge]&256)===256}var Gx="svg",Wx="math";function si(n){for(;Array.isArray(n);)n=n[vi];return n}function Um(n,e){return si(e[n])}function Wi(n,e){return si(e[n.index])}function ju(n,e){return n.data[e]}function ji(n,e){let t=e[n];return Vr(t)?t:t[vi]}function jx(n){return(n[Ge]&4)===4}function $u(n){return(n[Ge]&128)===128}function $x(n){return yi(n[Xt])}function $i(n,e){return e==null?null:n[e]}function km(n){n[Ns]=0}function Bm(n){n[Ge]&1024||(n[Ge]|=1024,$u(n)&&Io(n))}function qx(n,e){for(;n>0;)e=e[Rs],n--;return e}function ic(n){return!!(n[Ge]&9216||n[Gn]?.dirty)}function qu(n){n[zi].changeDetectionScheduler?.notify(8),n[Ge]&64&&(n[Ge]|=1024),ic(n)&&Io(n)}function Io(n){n[zi].changeDetectionScheduler?.notify(0);let e=Or(n);for(;e!==null&&!(e[Ge]&8192||(e[Ge]|=8192,!$u(e)));)e=Or(e)}function Vm(n,e){if(Os(n))throw new Ae(911,!1);n[lr]===null&&(n[lr]=[]),n[lr].push(e)}function Xx(n,e){if(n[lr]===null)return;let t=n[lr].indexOf(e);t!==-1&&n[lr].splice(t,1)}function Or(n){let e=n[Xt];return yi(e)?e[Xt]:e}function Hm(n){return n[wo]??=[]}function zm(n){return n.cleanup??=[]}function Yx(n,e,t,i){let r=Hm(e);r.push(t),n.firstCreatePass&&zm(n).push(i,r.length-1)}var Ze={lFrame:c0(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var dm=!1;function Zx(){return Ze.lFrame.elementDepthCount}function Kx(){Ze.lFrame.elementDepthCount++}function Gm(){Ze.lFrame.elementDepthCount--}function Jx(){return Ze.bindingsEnabled}function Qx(){return Ze.skipHydrationRootTNode!==null}function Wm(n){return Ze.skipHydrationRootTNode===n}function jm(){Ze.skipHydrationRootTNode=null}function lt(){return Ze.lFrame.lView}function oi(){return Ze.lFrame.tView}function Ao(n){return Ze.lFrame.contextLView=n,n[Vt]}function Do(n){return Ze.lFrame.contextLView=null,n}function ai(){let n=$m();for(;n!==null&&n.type===64;)n=n.parent;return n}function $m(){return Ze.lFrame.currentTNode}function e0(){let n=Ze.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Ro(n,e){let t=Ze.lFrame;t.currentTNode=n,t.isParent=e}function qm(){return Ze.lFrame.isParent}function t0(){Ze.lFrame.isParent=!1}function Xm(){return dm}function Xa(n){let e=dm;return dm=n,e}function Xu(){return Ze.lFrame.bindingIndex}function n0(n){return Ze.lFrame.bindingIndex=n}function rc(){return Ze.lFrame.bindingIndex++}function sc(n){let e=Ze.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function i0(){return Ze.lFrame.inI18n}function r0(n,e){let t=Ze.lFrame;t.bindingIndex=t.bindingRootIndex=n,Yu(e)}function s0(){return Ze.lFrame.currentDirectiveIndex}function Yu(n){Ze.lFrame.currentDirectiveIndex=n}function o0(n){let e=Ze.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Ym(){return Ze.lFrame.currentQueryIndex}function Zu(n){Ze.lFrame.currentQueryIndex=n}function XT(n){let e=n[Fe];return e.type===2?e.declTNode:e.type===1?n[ri]:null}function Zm(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=XT(s),r===null||(s=s[Rs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ze.lFrame=a0();return i.currentTNode=e,i.lView=n,!0}function Ku(n){let e=a0(),t=n[Fe];Ze.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function a0(){let n=Ze.lFrame,e=n===null?null:n.child;return e===null?c0(n):e}function c0(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function l0(){let n=Ze.lFrame;return Ze.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Km=l0;function Ju(){let n=l0();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function u0(n){return(Ze.lFrame.contextLView=qx(n,Ze.lFrame.contextLView))[Vt]}function _i(){return Ze.lFrame.selectedIndex}function zr(n){Ze.lFrame.selectedIndex=n}function d0(){let n=Ze.lFrame;return ju(n.tView,n.selectedIndex)}function Jm(){return Ze.lFrame.currentNamespace}var f0=!0;function Qu(){return f0}function ed(n){f0=n}function fm(n,e=null,t=null,i){let r=Qm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Qm(n,e=null,t=null,i,r=new Set){let s=[t||Hi,Ux(n)],o;return new Ts(s,e||tc(),o||null,r)}var gi=class n{static THROW_IF_NOT_FOUND=Ms;static NULL=new qa;static create(e,t){if(Array.isArray(e))return fm({name:""},t,e,"");{let i=e.name??"";return fm({name:i},e.parent,e.providers,i)}}static \u0275prov=Ue({token:n,providedIn:"any",factory:()=>Xe(Am)});static __NG_ELEMENT_ID__=-1},Zt=new Oe(""),hr=(()=>{class n{static __NG_ELEMENT_ID__=YT;static __NG_ENV_ID__=t=>t}return n})(),Du=class extends hr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Os(this._lView)}onDestroy(e){let t=this._lView;return Vm(t,e),()=>Xx(t,e)}};function YT(){return new Du(lt())}var h0=!1,p0=new Oe(""),Gr=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new cn(!1);debugTaskTracker=re(p0,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ft(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Ue({token:n,providedIn:"root",factory:()=>new n})}return n})(),hm=class extends an{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Hx()&&(this.destroyRef=re(hr,{optional:!0})??void 0,this.pendingTasks=re(Gr,{optional:!0})??void 0)}emit(e){let t=De(null);try{super.next(e)}finally{De(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof on&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},An=hm;function Ru(...n){}function eg(n){let e,t;function i(){n=Ru;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function m0(n){return queueMicrotask(()=>n()),()=>{n=Ru}}var tg="isAngularZone",Ya=tg+"_ID",ZT=0,Vn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new An(!1);onMicrotaskEmpty=new An(!1);onStable=new An(!1);onError=new An(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=h0}=e;if(typeof Zone>"u")throw new Ae(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,QT(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(tg)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Ae(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Ae(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,KT,Ru,Ru);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},KT={};function ng(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function JT(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){eg(()=>{n.callbackScheduled=!1,pm(n),n.isCheckStableRunning=!0,ng(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),pm(n)}function QT(n){let e=()=>{JT(n)},t=ZT++;n._inner=n._inner.fork({name:"angular",properties:{[tg]:!0,[Ya]:t,[Ya+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(eC(c))return i.invokeTask(s,o,a,c);try{return Cx(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Ix(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return Cx(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!tC(c)&&e(),Ix(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,pm(n),ng(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function pm(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Cx(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Ix(n){n._nesting--,ng(n)}var Za=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new An;onMicrotaskEmpty=new An;onStable=new An;onError=new An;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function eC(n){return g0(n,"__ignore_ng_zone__")}function tC(n){return g0(n,"__scheduler_tick__")}function g0(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ur=class{_console=console;handleError(e){this._console.error("ERROR",e)}},qi=new Oe("",{factory:()=>{let n=re(Vn),e=re(qt),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(ur),t.handleError(i))})}}}),v0={provide:Ds,useValue:()=>{let n=re(ur,{optional:!0})},multi:!0},nC=new Oe("",{factory:()=>{let n=re(Zt).defaultView;if(!n)return;let e=re(qi),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),re(hr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function ig(){return Mo([Fx(()=>{re(nC)})])}function Dn(n,e){let[t,i,r]=Hp(n,e?.equal),s=t,o=s[Cn];return s.set=i,s.update=r,s.asReadonly=y0.bind(s),s}function y0(){let n=this[Cn];if(n.readonlyFn===void 0){let e=()=>this();e[Cn]=n,n.readonlyFn=e}return n.readonlyFn}var td=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=iC}return n})();function iC(){return new td(lt(),ai())}var Cs=class{},oc=new Oe("",{factory:()=>!0});var rg=new Oe("");var nd=(()=>{class n{static \u0275prov=Ue({token:n,providedIn:"root",factory:()=>new mm})}return n})(),mm=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Nu=class{[Cn];constructor(e){this[Cn]=e}destroy(){this[Cn].destroy()}};function sg(n,e){let t=e?.injector??re(gi),i=e?.manualCleanup!==!0?t.get(hr):null,r,s=t.get(td,null,{optional:!0}),o=t.get(Cs);return s!==null?(r=oC(s.view,o,n),i instanceof Du&&i._lView===s.view&&(i=null)):r=aC(n,t.get(nd),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Nu(r)}var _0=tt(oe({},Gp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=Xa(!1);try{Wp(this)}finally{Xa(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=De(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],De(n)}}}),rC=tt(oe({},_0),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(ys(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),sC=tt(oe({},_0),{consumerMarkedDirty(){this.view[Ge]|=8192,Io(this.view),this.notifier.notify(13)},destroy(){if(ys(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Lr]?.delete(this)}});function oC(n,e,t){let i=Object.create(sC);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=x0(i,t),n[Lr]??=new Set,n[Lr].add(i),i.consumerMarkedDirty(i),i}function aC(n,e,t){let i=Object.create(rC);return i.fn=x0(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function x0(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function wd(n){return{toString:n}.toString()}function _C(n){return typeof n=="function"}function Y0(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var cd=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},Gg=(()=>{let n=()=>Z0;return n.ngInherit=!0,n})();function Z0(n){return n.type.prototype.ngOnChanges&&(n.setInput=SC),xC}function xC(){let n=J0(this),e=n?.current;if(e){let t=n.previous;if(t===As)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function SC(n,e,t,i,r){let s=this.declaredInputs[i],o=J0(n)||bC(n,{previous:As,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new cd(l&&l.currentValue,t,c===As),Y0(n,e,r,t)}var K0="__ngSimpleChanges__";function J0(n){return n[K0]||null}function bC(n,e){return n[K0]=e}var S0=[];var wt=function(n,e=null,t){for(let i=0;i<S0.length;i++){let r=S0[i];r(n,e,t)}},ht=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ht||{});function MC(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Z0(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function EC(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function rd(n,e,t){Q0(n,e,3,t)}function sd(n,e,t,i){(n[Ge]&3)===t&&Q0(n,e,t,i)}function og(n,e){let t=n[Ge];(t&3)===e&&(t&=16383,t+=1,n[Ge]=t)}function Q0(n,e,t,i){let r=i!==void 0?n[Ns]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Ns]+=65536),(a<s||s==-1)&&(wC(n,t,e,c),n[Ns]=(n[Ns]&4294901760)+c+2),c++}function b0(n,e){wt(ht.LifecycleHookStart,n,e);let t=De(null);try{e.call(n)}finally{De(t),wt(ht.LifecycleHookEnd,n,e)}}function wC(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Ge]>>14<n[Ns]>>16&&(n[Ge]&3)===e&&(n[Ge]+=16384,b0(a,s)):b0(a,s)}var Po=-1,uc=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function TC(n){return(n.flags&8)!==0}function CC(n){return(n.flags&16)!==0}function IC(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];DC(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function AC(n){return n===3||n===4||n===6}function DC(n){return n.charCodeAt(0)===64}function Td(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?M0(n,t,r,null,e[++i]):M0(n,t,r,null,null))}}return n}function M0(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function eS(n){return n!==Po}function ld(n){return n&32767}function RC(n){return n>>16}function ud(n,e){let t=RC(n),i=e;for(;t>0;)i=i[Rs],t--;return i}var hg=!0;function E0(n){let e=hg;return hg=n,e}var NC=256,tS=NC-1,nS=5,PC=0,Xi={};function LC(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Is)&&(i=t[Is]),i==null&&(i=t[Is]=PC++);let r=i&tS,s=1<<r;e.data[n+(r>>nS)]|=s}function iS(n,e){let t=rS(n,e);if(t!==-1)return t;let i=e[Fe];i.firstCreatePass&&(n.injectorIndex=e.length,ag(i.data,n),ag(e,null),ag(i.blueprint,null));let r=Wg(n,e),s=n.injectorIndex;if(eS(r)){let o=ld(r),a=ud(r,e),c=a[Fe].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function ag(n,e){n.push(0,0,0,0,0,0,0,0,e)}function rS(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Wg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=lS(r),i===null)return Po;if(t++,r=r[Rs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Po}function OC(n,e,t){LC(n,e,t)}function sS(n,e,t){if(t&8||n!==void 0)return n;ku(e,"NodeInjector")}function oS(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[dr],s=Bn(void 0);try{return r?r.get(e,i,t&8):Cm(e,i,t&8)}finally{Bn(s)}}return sS(i,e,t)}function aS(n,e,t,i=0,r){if(n!==null){if(e[Ge]&2048&&!(i&2)){let o=BC(n,e,t,i,Xi);if(o!==Xi)return o}let s=cS(n,e,t,i,Xi);if(s!==Xi)return s}return oS(e,t,i,r)}function cS(n,e,t,i,r){let s=UC(t);if(typeof s=="function"){if(!Zm(e,n,i))return i&1?sS(r,t,i):oS(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))ku(t);else return o}finally{Km()}}else if(typeof s=="number"){let o=null,a=rS(n,e),c=Po,l=i&1?e[zn][ri]:null;for((a===-1||i&4)&&(c=a===-1?Wg(n,e):e[a+8],c===Po||!T0(i,!1)?a=-1:(o=e[Fe],a=ld(c),e=ud(c,e)));a!==-1;){let u=e[Fe];if(w0(s,a,u.data)){let d=FC(a,e,t,o,i,l);if(d!==Xi)return d}c=e[a+8],c!==Po&&T0(i,e[Fe].data[a+8]===l)&&w0(s,a,e)?(o=u,a=ld(c),e=ud(c,e)):a=-1}}return r}function FC(n,e,t,i,r,s){let o=e[Fe],a=o.data[n+8],c=i==null?Hr(a)&&hg:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=od(a,o,t,c,l);return u!==null?dd(e,o,u,a,r):Xi}function od(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let m=o[h];if(h<c&&t===m||h>=c&&m.type===t)return h}if(r){let h=o[c];if(h&&Ls(h)&&h.type===t)return c}return null}function dd(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof uc){let a=s;if(a.resolving)throw Tm("");let c=E0(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?Bn(a.injectImpl):null,f=Zm(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&MC(t,o[t],e)}finally{d!==null&&Bn(d),E0(c),a.resolving=!1,Km()}}return s}function UC(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Is)?n[Is]:void 0;return typeof e=="number"?e>=0?e&tS:kC:e}function w0(n,e,t){let i=1<<n;return!!(t[e+(n>>nS)]&i)}function T0(n,e){return!(n&2)&&!(n&1&&e)}var Fs=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return aS(this._tNode,this._lView,e,Es(i),t)}};function kC(){return new Fs(ai(),lt())}function yc(n){return wd(()=>{let e=n.prototype.constructor,t=e[$a]||pg(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[$a]||pg(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function pg(n){return vm(n)?()=>{let e=pg(Fn(n));return e&&e()}:ws(n)}function BC(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Ge]&2048&&!Co(o);){let a=cS(s,o,t,i|2,Xi);if(a!==Xi)return a;let c=s.parent;if(!c){let l=o[Lm];if(l){let u=l.get(t,Xi,i&-5);if(u!==Xi)return u}c=lS(o),o=o[Rs]}s=c}return r}function lS(n){let e=n[Fe],t=e.type;return t===2?e.declTNode:t===1?n[ri]:null}function VC(){return ko(ai(),lt())}function ko(n,e){return new _c(Wi(n,e))}var _c=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=VC}return n})();function HC(n){return n instanceof _c?n.nativeElement:n}function zC(){return this._results[Symbol.iterator]()}var fd=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new an}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Lx(e);(this._changesDetected=!Px(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=zC};function uS(n){return(n.flags&128)===128}var jg=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(jg||{}),dS=new Map,GC=0;function WC(){return GC++}function jC(n){dS.set(n[fr],n)}function mg(n){dS.delete(n[fr])}var C0="__ngContext__";function Lo(n,e){Vr(e)?(n[C0]=e[fr],jC(e)):n[C0]=e}function fS(n){return pS(n[To])}function hS(n){return pS(n[ii])}function pS(n){for(;n!==null&&!yi(n);)n=n[ii];return n}var $C;function $g(n){$C=n}var Cd=new Oe("",{factory:()=>qC}),qC="ng";var Id=new Oe(""),xc=new Oe("",{providedIn:"platform",factory:()=>"unknown"});var Ad=new Oe("",{factory:()=>re(Zt).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var mS="r";var gS="di";var vS=!1,yS=new Oe("",{factory:()=>vS});var I0=new WeakMap;function XC(n,e){if(n==null||typeof n!="object")return;let t=I0.get(n);t||(t=new WeakSet,I0.set(n,t)),t.add(e)}var YC=(n,e,t,i)=>{};function ZC(n,e,t,i){YC(n,e,t,i)}function qg(n){return(n.flags&32)===32}var KC=()=>null;function _S(n,e,t=!1){return KC(n,e,t)}function xS(n,e){let t=n.contentQueries;if(t!==null){let i=De(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Zu(s),a.contentQueries(2,e[o],o)}}}finally{De(i)}}}function gg(n,e,t){Zu(0);let i=De(null);try{e(n,t)}finally{De(i)}}function SS(n,e,t){if(Fm(e)){let i=De(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{De(i)}}}var bi=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(bi||{});var vg=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${gm})`}};function bS(n){return n instanceof vg?n.changingThisBreaksApplicationSecurity:n}function JC(n,e){return n.createText(e)}function QC(n,e,t){n.setValue(e,t)}function MS(n,e,t){return n.createElement(e,t)}function hd(n,e,t,i,r){n.insertBefore(e,t,i,r)}function ES(n,e,t){n.appendChild(e,t)}function A0(n,e,t,i,r){i!==null?hd(n,e,t,i,r):ES(n,e,t)}function wS(n,e,t,i){n.removeChild(null,e,t,i)}function eI(n,e,t){n.setAttribute(e,"style",t)}function tI(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function TS(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&IC(n,e,i),r!==null&&tI(n,e,r),s!==null&&eI(n,e,s)}function CS(n){return n instanceof Function?n():n}function nI(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var IS="ng-template";function iI(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&nI(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Xg(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Xg(n){return n.type===4&&n.value!==IS}function rI(n,e,t){let i=n.type===4&&!t?IS:n.value;return e===i}function sI(n,e,t){let i=4,r=n.attrs,s=r!==null?cI(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!xi(i)&&!xi(c))return!1;if(o&&xi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!rI(n,c,t)||c===""&&e.length===1){if(xi(i))return!1;o=!0}}else if(i&8){if(r===null||!iI(n,r,c,t)){if(xi(i))return!1;o=!0}}else{let l=e[++a],u=oI(c,r,Xg(n),t);if(u===-1){if(xi(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(xi(i))return!1;o=!0}}}}return xi(i)||o}function xi(n){return(n&1)===0}function oI(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return lI(e,n)}function aI(n,e,t=!1){for(let i=0;i<e.length;i++)if(sI(n,e[i],t))return!0;return!1}function cI(n){for(let e=0;e<n.length;e++){let t=n[e];if(AC(t))return e}return n.length}function lI(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function D0(n,e){return n?":not("+e.trim()+")":e}function uI(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!xi(o)&&(e+=D0(s,r),r=""),i=o,s=s||!xi(i);t++}return r!==""&&(e+=D0(s,r)),e}function dI(n){return n.map(uI).join(",")}function fI(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!xi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var _n={};function Yg(n,e,t,i,r,s,o,a,c,l,u){let d=un+i,f=d+r,h=hI(d,f),m=typeof l=="function"?l():l;return h[Fe]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:u}}function hI(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:_n);return t}function pI(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Yg(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Zg(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[vi]=r,d[Ge]=i|4|128|8|64|1024,(l!==null||n&&n[Ge]&2048)&&(d[Ge]|=2048),km(d),d[Xt]=d[Rs]=n,d[Vt]=t,d[zi]=o||n&&n[zi],d[Yt]=a||n&&n[Yt],d[dr]=c||n&&n[dr]||null,d[ri]=s,d[fr]=WC(),d[Eo]=u,d[Lm]=l,d[zn]=e.type==2?n[zn]:d,d}function mI(n,e,t){let i=Wi(e,n),r=pI(t),s=n[zi].rendererFactory,o=Kg(n,Zg(n,r,null,AS(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function AS(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function DS(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Kg(n,e){return n[To]?n[Pm][ii]=e:n[To]=e,n[Pm]=e,e}function at(n=1){RS(oi(),lt(),_i()+n,!1)}function RS(n,e,t,i){if(!i)if((e[Ge]&3)===3){let s=n.preOrderCheckHooks;s!==null&&rd(e,s,t)}else{let s=n.preOrderHooks;s!==null&&sd(e,s,0,t)}zr(t)}var Dd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Dd||{});function yg(n,e,t,i){let r=De(null);try{let[s,o,a]=n.inputs[t],c=null;(o&Dd.SignalBased)!==0&&(c=e[s][Cn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Y0(e,c,s,i)}finally{De(r)}}var pr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(pr||{}),gI;function Jg(n,e){return gI(n,e)}var OG=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var _g=new WeakMap,ac=new WeakSet;function vI(n,e){let t=_g.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),ac.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function yI(n,e){let t=_g.get(n);t?t.includes(e)||t.push(e):_g.set(n,[e])}var Us=new Set,Qg=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Qg||{}),Bo=new Oe(""),R0=new Set;function Vs(n){R0.has(n)||(R0.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var NS=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Ue({token:n,providedIn:"root",factory:()=>new n})}return n})();var PS=new Oe("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:re(qt)})});function LS(n,e,t){let i=n.get(PS);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function _I(n,e){let t=n.get(PS);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function xI(n,e){for(let[t,i]of e)LS(n,i.animateFns)}function N0(n,e,t,i){let r=n?.[kr]?.enter;e!==null&&r&&r.has(t.index)&&xI(i,r)}function No(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;yi(r)?c=r:Vr(r)&&(l=!0,r=r[vi]);let u=si(r);n===0&&i!==null?(N0(a,i,s,t),o==null?ES(e,i,u):hd(e,i,u,o||null,!0)):n===1&&i!==null?(N0(a,i,s,t),hd(e,i,u,o||null,!0),vI(s,u)):n===2?(a?.[kr]?.leave?.has(s.index)&&yI(s,u),ac.delete(u),P0(a,s,t,d=>{if(ac.has(u)){ac.delete(u);return}wS(e,u,l,d)})):n===3&&(ac.delete(u),P0(a,s,t,()=>{e.destroyNode(u)})),c!=null&&PI(e,n,t,c,s,i,o)}}function SI(n,e){OS(n,e),e[vi]=null,e[ri]=null}function bI(n,e,t,i,r,s){i[vi]=r,i[ri]=e,Nd(n,i,t,1,r,s)}function OS(n,e){e[zi].changeDetectionScheduler?.notify(9),Nd(n,e,e[Yt],2,null,null)}function MI(n){let e=n[To];if(!e)return cg(n[Fe],n);for(;e;){let t=null;if(Vr(e))t=e[To];else{let i=e[Ut];i&&(t=i)}if(!t){for(;e&&!e[ii]&&e!==n;)Vr(e)&&cg(e[Fe],e),e=e[Xt];e===null&&(e=n),Vr(e)&&cg(e[Fe],e),t=e&&e[ii]}e=t}}function ev(n,e){let t=n[Ps],i=t.indexOf(e);t.splice(i,1)}function Rd(n,e){if(Os(e))return;let t=e[Yt];t.destroyNode&&Nd(n,e,t,3,null,null),MI(e)}function cg(n,e){if(Os(e))return;let t=De(null);try{e[Ge]&=-129,e[Ge]|=256,e[Gn]&&ys(e[Gn]),TI(n,e),wI(n,e),e[Fe].type===1&&e[Yt].destroy();let i=e[Ur];if(i!==null&&yi(e[Xt])){i!==e[Xt]&&ev(i,e);let r=e[Gi];r!==null&&r.detachView(n)}mg(e)}finally{De(t)}}function P0(n,e,t,i){let r=n?.[kr];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&Us.add(n[fr]),LS(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),EI(n,i)}else n&&Us.delete(n[fr]),i(!1)},r)}function EI(n,e){let t=n[kr]?.running;if(t){t.then(()=>{n[kr].running=void 0,Us.delete(n[fr]),e(!0)});return}e(!1)}function wI(n,e){let t=n.cleanup,i=e[wo];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[wo]=null);let r=e[lr];if(r!==null){e[lr]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Lr];if(s!==null){e[Lr]=null;for(let o of s)o.destroy()}}function TI(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof uc)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];wt(ht.LifecycleHookStart,a,c);try{c.call(a)}finally{wt(ht.LifecycleHookEnd,a,c)}}else{wt(ht.LifecycleHookStart,r,s);try{s.call(r)}finally{wt(ht.LifecycleHookEnd,r,s)}}}}}function CI(n,e,t){return II(n,e.parent,t)}function II(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[vi];if(Hr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===bi.None||r===bi.Emulated)return null}return Wi(i,t)}function AI(n,e,t){return RI(n,e,t)}function DI(n,e,t){return n.type&40?Wi(n,t):null}var RI=DI,L0;function tv(n,e,t,i){let r=CI(n,i,e),s=e[Yt],o=i.parent||e[ri],a=AI(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)A0(s,r,t[c],a,!1);else A0(s,r,t,a,!1);L0!==void 0&&L0(s,i,e,t,r)}function cc(n,e){if(e!==null){let t=e.type;if(t&3)return Wi(e,n);if(t&4)return xg(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return cc(n,i);{let r=n[e.index];return yi(r)?xg(-1,r):si(r)}}else{if(t&128)return cc(n,e.next);if(t&32)return Jg(e,n)()||si(n[e.index]);{let i=FS(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Or(n[zn]);return cc(r,i)}else return cc(n,e.next)}}}return null}function FS(n,e){if(e!==null){let i=n[zn][ri],r=e.projection;return i.projection[r]}return null}function xg(n,e){let t=Ut+n+1;if(t<e.length){let i=e[t],r=i[Fe].firstChild;if(r!==null)return cc(i,r)}return e[Br]}function nv(n,e,t,i,r,s,o){for(;t!=null;){let a=i[dr];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Lo(si(c),i),t.flags|=2),!qg(t))if(l&8)nv(n,e,t.child,i,r,s,!1),No(e,n,a,r,c,t,s,i);else if(l&32){let u=Jg(t,i),d;for(;d=u();)No(e,n,a,r,d,t,s,i);No(e,n,a,r,c,t,s,i)}else l&16?NI(n,e,i,t,r,s):No(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Nd(n,e,t,i,r,s){nv(t,i,n.firstChild,e,r,s,!1)}function NI(n,e,t,i,r,s){let o=t[zn],c=o[ri].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];No(e,n,t[dr],r,u,i,s,t)}else{let l=c,u=o[Xt];uS(i)&&(l.flags|=128),nv(n,e,l,u,r,s,!0)}}function PI(n,e,t,i,r,s,o){let a=i[Br],c=si(i);a!==c&&No(e,n,t,s,a,r,o);for(let l=Ut;l<i.length;l++){let u=i[l];Nd(u[Fe],u,n,e,s,a)}}function LI(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:pr.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=pr.Important),n.setStyle(t,i,r,s))}}function US(n,e,t,i,r){let s=_i(),o=i&2;try{zr(-1),o&&e.length>un&&RS(n,e,un,!1);let a=o?ht.TemplateUpdateStart:ht.TemplateCreateStart;wt(a,r,t),t(i,r)}finally{zr(s);let a=o?ht.TemplateUpdateEnd:ht.TemplateCreateEnd;wt(a,r,t)}}function kS(n,e,t){BI(n,e,t),(t.flags&64)===64&&VI(n,e,t)}function iv(n,e,t=Wi){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function OI(n,e,t,i){let s=i.get(yS,vS)||t===bi.ShadowDom||t===bi.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return FI(o),o}function FI(n){UI(n)}var UI=()=>null;function kI(n,e,t,i,r,s){if(n.type&3){let o=Wi(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function BI(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Hr(t)&&mI(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||iS(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=dd(e,n,o,t);if(Lo(c,e),s!==null&&GI(e,o-i,c,a,t,s),Ls(a)){let l=ji(t.index,e);l[Vt]=dd(e,n,o,t)}}}function VI(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=s0();try{zr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Yu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&HI(c,l)}}finally{zr(-1),Yu(o)}}function HI(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function zI(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];aI(e,s.selectors,!1)&&(i??=[],Ls(s)?i.unshift(s):i.push(s))}return i}function GI(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];yg(i,t,c,l)}}function BS(n,e,t,i,r){let s=un+t,o=e[Fe],a=r(o,e,n,i,t);e[s]=a,Ro(n,!0);let c=n.type===2;return c?(TS(e[Yt],a,n),(Zx()===0||Wu(n))&&Lo(a,e),Kx()):Lo(a,e),Qu()&&(!c||!qg(n))&&tv(o,e,a,n),n}function VS(n){let e=n;return qm()?t0():(e=e.parent,Ro(e,!1)),e}function WI(n,e){let t=n[dr];if(!t)return;let i;try{i=t.get(qi,null)}catch{i=null}i?.(e)}function HS(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];yg(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];yg(u,l,i,r),a=!0}return a}function jI(n,e){let t=ji(e,n),i=t[Fe];$I(i,t);let r=t[vi];r!==null&&t[Eo]===null&&(t[Eo]=_S(r,t[dr])),wt(ht.ComponentStart);try{rv(i,t,t[Vt])}finally{wt(ht.ComponentEnd,t[Vt])}}function $I(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function rv(n,e,t){Ku(e);try{let i=n.viewQuery;i!==null&&gg(1,i,t);let r=n.template;r!==null&&US(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Gi]?.finishViewCreation(n),n.staticContentQueries&&xS(n,e),n.staticViewQueries&&gg(2,n.viewQuery,t);let s=n.components;s!==null&&qI(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Ge]&=-5,Ju()}}function qI(n,e){for(let t=0;t<e.length;t++)jI(n,e[t])}function Pd(n,e,t,i){let r=De(null);try{let s=e.tView,a=n[Ge]&4096?4096:16,c=Zg(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Ur]=l;let u=n[Gi];return u!==null&&(c[Gi]=u.createEmbeddedView(s)),rv(s,c,t),c}finally{De(r)}}function dc(n,e){return!e||e.firstChild===null||uS(n)}function fc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(si(s)),yi(s)&&zS(s,i);let o=t.type;if(o&8)fc(n,e,t.child,i);else if(o&32){let a=Jg(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=FS(e,t);if(Array.isArray(a))i.push(...a);else{let c=Or(e[zn]);fc(c[Fe],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function zS(n,e){for(let t=Ut;t<n.length;t++){let i=n[t],r=i[Fe].firstChild;r!==null&&fc(i[Fe],i,r,e)}n[Br]!==n[vi]&&e.push(n[Br])}function GS(n){if(n[Gu]!==null){for(let e of n[Gu])e.impl.addSequence(e);n[Gu].length=0}}var WS=[];function XI(n){return n[Gn]??YI(n)}function YI(n){let e=WS.pop()??Object.create(KI);return e.lView=n,e}function ZI(n){n.lView[Gn]!==n&&(n.lView=null,WS.push(n))}var KI=tt(oe({},gs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Io(n.lView)},consumerOnSignalRead(){this.lView[Gn]=this}});function JI(n){let e=n[Gn]??Object.create(QI);return e.lView=n,e}var QI=tt(oe({},gs),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Or(n.lView);for(;e&&!jS(e[Fe]);)e=Or(e);e&&Bm(e)},consumerOnSignalRead(){this.lView[Gn]=this}});function jS(n){return n.type!==2}function $S(n){if(n[Lr]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Lr])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Ge]&8192)}}var eA=100;function qS(n,e=0){let i=n[zi].rendererFactory,r=!1;r||i.begin?.();try{tA(n,e)}finally{r||i.end?.()}}function tA(n,e){let t=Xm();try{Xa(!0),Sg(n,e);let i=0;for(;ic(n);){if(i===eA)throw new Ae(103,!1);i++,Sg(n,1)}}finally{Xa(t)}}function nA(n,e,t,i){if(Os(e))return;let r=e[Ge],s=!1,o=!1;Ku(e);let a=!0,c=null,l=null;s||(jS(n)?(l=XI(e),c=vs(l)):Ql()===null?(a=!1,l=JI(e),c=vs(l)):e[Gn]&&(ys(e[Gn]),e[Gn]=null));try{km(e),n0(n.bindingStartIndex),t!==null&&US(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&rd(e,h,null)}else{let h=n.preOrderHooks;h!==null&&sd(e,h,0,null),og(e,0)}if(o||iA(e),$S(e),XS(e,0),n.contentQueries!==null&&xS(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&rd(e,h)}else{let h=n.contentHooks;h!==null&&sd(e,h,1),og(e,1)}sA(n,e);let d=n.components;d!==null&&ZS(e,d,0);let f=n.viewQuery;if(f!==null&&gg(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&rd(e,h)}else{let h=n.viewHooks;h!==null&&sd(e,h,2),og(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[zu]){for(let h of e[zu])h();e[zu]=null}s||(GS(e),e[Ge]&=-73)}catch(u){throw s||Io(e),u}finally{l!==null&&(uo(l,c),a&&ZI(l)),Ju()}}function XS(n,e){for(let t=fS(n);t!==null;t=hS(t))for(let i=Ut;i<t.length;i++){let r=t[i];YS(r,e)}}function iA(n){for(let e=fS(n);e!==null;e=hS(e)){if(!(e[Ge]&2))continue;let t=e[Ps];for(let i=0;i<t.length;i++){let r=t[i];Bm(r)}}}function rA(n,e,t){wt(ht.ComponentStart);let i=ji(e,n);try{YS(i,t)}finally{wt(ht.ComponentEnd,i[Vt])}}function YS(n,e){$u(n)&&Sg(n,e)}function Sg(n,e){let i=n[Fe],r=n[Ge],s=n[Gn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Ba(s)),o||=!1,s&&(s.dirty=!1),n[Ge]&=-9217,o)nA(i,n,i.template,n[Vt]);else if(r&8192){let a=De(null);try{$S(n),XS(n,1);let c=i.components;c!==null&&ZS(n,c,1),GS(n)}finally{De(a)}}}function ZS(n,e,t){for(let i=0;i<e.length;i++)rA(n,e[i],t)}function sA(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)zr(~r);else{let s=r,o=t[++i],a=t[++i];r0(o,s);let c=e[s];wt(ht.HostBindingsUpdateStart,c);try{a(2,c)}finally{wt(ht.HostBindingsUpdateEnd,c)}}}}finally{zr(-1)}}function sv(n,e){let t=Xm()?64:1088;for(n[zi].changeDetectionScheduler?.notify(e);n;){n[Ge]|=t;let i=Or(n);if(Co(n)&&!i)return n;n=i}return null}function KS(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function JS(n,e){let t=Ut+e;if(t<n.length)return n[t]}function Ld(n,e,t,i=!0){let r=e[Fe];if(oA(r,e,n,t),i){let o=xg(t,n),a=e[Yt],c=a.parentNode(n[Br]);c!==null&&bI(r,n[ri],a,e,c,o)}let s=e[Eo];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function QS(n,e){let t=hc(n,e);return t!==void 0&&Rd(t[Fe],t),t}function hc(n,e){if(n.length<=Ut)return;let t=Ut+e,i=n[t];if(i){let r=i[Ur];r!==null&&r!==n&&ev(r,i),e>0&&(n[t-1][ii]=i[ii]);let s=Qa(n,Ut+e);SI(i[Fe],i);let o=s[Gi];o!==null&&o.detachView(s[Fe]),i[Xt]=null,i[ii]=null,i[Ge]&=-129}return i}function oA(n,e,t,i){let r=Ut+i,s=t.length;i>0&&(t[r-1][ii]=e),i<s-Ut?(e[ii]=t[r],Im(t,Ut+i,e)):(t.push(e),e[ii]=null),e[Xt]=t;let o=e[Ur];o!==null&&t!==o&&eb(o,e);let a=e[Gi];a!==null&&a.insertView(n),qu(e),e[Ge]|=128}function eb(n,e){let t=n[Ps],i=e[Xt];if(Vr(i))n[Ge]|=2;else{let r=i[Xt][zn];e[zn]!==r&&(n[Ge]|=2)}t===null?n[Ps]=[e]:t.push(e)}var Wr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Fe];return fc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[Vt]}set context(e){this._lView[Vt]=e}get destroyed(){return Os(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Xt];if(yi(e)){let t=e[nc],i=t?t.indexOf(this):-1;i>-1&&(hc(e,i),Qa(t,i))}this._attachedToViewContainer=!1}Rd(this._lView[Fe],this._lView)}onDestroy(e){Vm(this._lView,e)}markForCheck(){sv(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Ge]&=-129}reattach(){qu(this._lView),this._lView[Ge]|=128}detectChanges(){this._lView[Ge]|=1024,qS(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Ae(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Co(this._lView),t=this._lView[Ur];t!==null&&!e&&ev(t,this._lView),OS(this._lView[Fe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Ae(902,!1);this._appRef=e;let t=Co(this._lView),i=this._lView[Ur];i!==null&&!t&&eb(i,this._lView),qu(this._lView)}};var pc=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=aA;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=Pd(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Wr(s)}}return n})();function aA(){return ov(ai(),lt())}function ov(n,e){return n.type&4?new pc(e,n,ko(n,e)):null}function Od(n,e,t,i,r){let s=n.data[e];if(s===null)s=cA(n,e,t,i,r),i0()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=e0();s.injectorIndex=o===null?-1:o.injectorIndex}return Ro(s,!0),s}function cA(n,e,t,i,r){let s=$m(),o=qm(),a=o?s:s&&s.parent,c=n.data[e]=uA(n,a,t,e,i,r);return lA(n,c,s,o),c}function lA(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function uA(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return Qx()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Jm(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function dA(n){let e=n[Om]??[],i=n[Xt][Yt],r=[];for(let s of e)s.data[gS]!==void 0?r.push(s):fA(s,i);n[Om]=r}function fA(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[mS];for(;t<r;){let s=i.nextSibling;wS(e,i,!1),i=s,t++}}}var hA=()=>null,pA=()=>null;function bg(n,e){return hA(n,e)}function tb(n,e,t){return pA(n,e,t)}var nb=class{},Fd=class{},Mg=class{resolveComponentFactory(e){throw new Ae(917,!1)}},Sc=class{static NULL=new Mg},ks=class{};var ib=(()=>{class n{static \u0275prov=Ue({token:n,providedIn:"root",factory:()=>null})}return n})();var ad={},Eg=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,ad,i);return r!==ad||t===ad?r:this.parentInjector.get(e,t,i)}};function pd(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Lu(r,a);else if(s==2){let c=a,l=e[++o];i=Lu(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function rb(n,e=0){let t=lt();if(t===null)return Xe(n,e);let i=ai();return aS(i,t,Fn(n),e)}function mA(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}yA(n,e,t,a,s,c,l)}s!==null&&i!==null&&gA(t,i,s)}function gA(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Ae(-301,!1);i.push(e[r],s)}}function vA(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function yA(n,e,t,i,r,s,o){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Ls(h)&&(c=h,vA(n,t,f)),OC(iS(t,e),n,h.type)}EA(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=DS(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=Td(t.mergedAttrs,h.hostAttrs),xA(n,t,e,d,h),MA(d,h,r),o!==null&&o.has(h)){let[y,g]=o.get(h);t.directiveToIndex.set(h.type,[d,y+t.directiveStart,g+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let m=h.type.prototype;!l&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(m.ngOnChanges||m.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}_A(n,t,s)}function _A(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))O0(0,e,r,i),O0(1,e,r,i),U0(e,i,!1);else{let s=t.get(r);F0(0,e,s,i),F0(1,e,s,i),U0(e,i,!0)}}}function O0(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),sb(e,s)}}function F0(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),sb(e,o)}}function sb(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function U0(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Xg(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function xA(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=ws(r.type,!0)),o=new uc(s,Ls(r),rb,null);n.blueprint[i]=o,t[i]=o,SA(n,e,i,DS(n,t,r.hostVars,_n),r)}function SA(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;bA(o)!=a&&o.push(a),o.push(t,i,s)}}function bA(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function MA(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Ls(e)&&(t[""]=n)}}function EA(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function ob(n,e,t,i,r,s,o,a){let c=e[Fe],l=c.consts,u=$i(l,o),d=Od(c,n,t,i,u);return s&&mA(c,e,d,$i(l,a),r),d.mergedAttrs=Td(d.mergedAttrs,d.attrs),d.attrs!==null&&pd(d,d.attrs,!1),d.mergedAttrs!==null&&pd(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function ab(n,e){EC(n,e),Fm(e)&&n.queries.elementEnd(e)}function wA(n,e,t,i,r,s){let o=e.consts,a=$i(o,r),c=Od(e,n,t,i,a);if(c.mergedAttrs=Td(c.mergedAttrs,c.attrs),s!=null){let l=$i(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&pd(c,c.attrs,!1),c.mergedAttrs!==null&&pd(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function jr(n,e,t){if(t===_n)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function mc(n,e,t,i){let r=jr(n,e,t);return jr(n,e+1,i)||r}function TA(n,e,t,i,r){let s=mc(n,e,t,i);return jr(n,e+2,r)||s}function CA(n,e,t,i,r,s){let o=mc(n,e,t,i);return mc(n,e+2,r,s)||o}function IA(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&XC(r,s);let o=Hr(n)?ji(n.index,e):e;sv(o,5);let a=e[Vt],c=k0(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=k0(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function k0(n,e,t,i){let r=De(null);try{return wt(ht.OutputStart,e,t),t(i)!==!1}catch(s){return WI(n,s),!1}finally{wt(ht.OutputEnd,e,t),De(r)}}function AA(n,e,t,i,r,s,o,a){let c=Wu(n),l=!1,u=null;if(!i&&c&&(u=RA(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=Wi(n,t),f=i?i(d):d;ZC(t,f,s,a),i||(a.__ngNativeEl__=d);let h=r.listen(f,s,a);if(!DA(s)){let m=i?y=>i(si(y[n.index])):n.index;NA(m,e,t,s,a,h,!1)}}return l}function DA(n){return n.startsWith("animation")||n.startsWith("transition")}function RA(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[wo],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function NA(n,e,t,i,r,s,o){let a=e.firstCreatePass?zm(e):null,c=Hm(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}var wg=Symbol("BINDING");function cb(n){return n.debugInfo?.className||n.type.name||null}var md=class extends Sc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Fr(e);return new Oo(t,this.ngModule)}};function PA(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Dd.SignalBased)!==0};return r&&(s.transform=r),s})}function LA(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function OA(n,e,t){let i=e instanceof qt?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Eg(t,i):t}function FA(n){let e=n.get(ks,null);if(e===null)throw new Ae(407,!1);let t=n.get(ib,null),i=n.get(Cs,null),r=n.get(Bo,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function UA(n,e){let t=lb(n);return MS(e,t,t==="svg"?Gx:t==="math"?Wx:null)}function kA(n){if(n?.toLowerCase()==="script")throw new Ae(905,!1)}function lb(n){return(n.selectors[0][0]||"div").toLowerCase()}var Oo=class extends Fd{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=PA(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=LA(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=dI(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){wt(ht.DynamicComponentStart);let a=De(null);try{let c=this.componentDef,l=OA(c,r||this.ngModule,e),u=FA(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(cb(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{De(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=BA(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?OI(l,r,a.encapsulation,t):UA(a,l);kA(u?.tagName);let d=o?.some(B0)||s?.some(m=>typeof m!="function"&&m.bindings.some(B0)),f=Zg(null,c,null,512|AS(a),null,null,e,l,t,null,_S(u,t,!0));f[un]=u,Ku(f);let h=null;try{let m=ob(un,f,2,"#host",()=>c.directiveRegistry,!0,0);TS(l,u,m),Lo(u,f),kS(c,f,m),SS(c,m,f),ab(c,m),i!==void 0&&HA(m,this.ngContentSelectors,i),h=ji(m.index,f),f[Vt]=h[Vt],rv(c,f,null)}catch(m){throw h!==null&&mg(h),mg(f),m}finally{wt(ht.DynamicComponentEnd),Ju()}return new gd(this.componentType,f,!!d)}};function BA(n,e,t,i){let r=n?["ng-version","21.2.25"]:fI(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[wg].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[wg].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(s??=[]).push(f)),f.update&&(f.targetIdx=h,(o??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=wm(d);c.push(f)}return Yg(0,null,VA(s,o),1,a,c,null,null,null,[r],null)}function VA(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function B0(n){let e=n[wg].kind;return e==="input"||e==="twoWay"}var gd=class extends nb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=ju(t[Fe],un),this.location=ko(this._tNode,t),this.instance=ji(this._tNode.index,t)[Vt],this.hostView=this.changeDetectorRef=new Wr(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=HS(i,r[Fe],r,e,t);this.previousInputValues.set(e,t);let o=ji(i.index,r);sv(o,1)}get injector(){return new Fs(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function HA(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Vo=(()=>{class n{static __NG_ELEMENT_ID__=zA}return n})();function zA(){let n=ai();return ub(n,lt())}var Tg=class n extends Vo{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return ko(this._hostTNode,this._hostLView)}get injector(){return new Fs(this._hostTNode,this._hostLView)}get parentInjector(){let e=Wg(this._hostTNode,this._hostLView);if(eS(e)){let t=ud(e,this._hostLView),i=ld(e),r=t[Fe].data[i+8];return new Fs(r,t)}else return new Fs(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=V0(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Ut}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=bg(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,dc(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!_C(e),l;if(c)l=t;else{let g=t||{};l=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef,o=g.directives,a=g.bindings}let u=c?e:new Oo(Fr(e)),d=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?d:this.parentInjector).get(qt,null);p&&(s=p)}let f=Fr(u.componentType??{}),h=bg(this._lContainer,f?.id??null),m=h?.firstChild??null,y=u.create(d,r,m,s,o,a);return this.insertImpl(y.hostView,l,dc(this._hostTNode,h)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if($x(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Xt],l=new n(c,c[ri],c[Xt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Ld(o,r,s,i),e.attachToViewContainerRef(),Im(lg(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=V0(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=hc(this._lContainer,t);i&&(Qa(lg(this._lContainer),t),Rd(i[Fe],i))}detach(e){let t=this._adjustIndex(e,-1),i=hc(this._lContainer,t);return i&&Qa(lg(this._lContainer),t)!=null?new Wr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function V0(n){return n[nc]}function lg(n){return n[nc]||(n[nc]=[])}function ub(n,e){let t,i=e[n.index];return yi(i)?t=i:(t=KS(i,e,null,n),e[n.index]=t,Kg(e,t)),WA(t,e,n,i),new Tg(t,n,e)}function GA(n,e){let t=n[Yt],i=t.createComment(""),r=Wi(e,n),s=t.parentNode(r);return hd(t,s,i,t.nextSibling(r),!1),i}var WA=qA,jA=()=>!1;function $A(n,e,t){return jA(n,e,t)}function qA(n,e,t,i){if(n[Br])return;let r;t.type&8?r=si(i):r=GA(e,t),n[Br]=r}var Cg=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Ig=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)av(e,t).matches!==null&&this.queries[t].setDirty()}},Ag=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=tD(e):this.predicate=e}},Dg=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Rg=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,XA(t,s)),this.matchTNodeWithReadOption(e,t,od(t,e,s,!1,!1))}else i===pc?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,od(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===_c||r===Vo||r===pc&&t.type&4)this.addMatch(t.index,-2);else{let s=od(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function XA(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function YA(n,e){return n.type&11?ko(n,e):n.type&4?ov(n,e):null}function ZA(n,e,t,i){return t===-1?YA(e,n):t===-2?KA(n,e,i):dd(n,n[Fe],t,e)}function KA(n,e,t){if(t===_c)return ko(e,n);if(t===pc)return ov(e,n);if(t===Vo)return ub(e,n)}function db(n,e,t,i){let r=e[Gi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(ZA(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Ng(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=db(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=Ut;d<u.length;d++){let f=u[d];f[Ur]===f[Xt]&&Ng(f[Fe],f,l,i)}if(u[Ps]!==null){let d=u[Ps];for(let f=0;f<d.length;f++){let h=d[f];Ng(h[Fe],h,l,i)}}}}}return i}function JA(n,e){return n[Gi].queries[e].queryList}function QA(n,e,t){let i=new fd((t&4)===4);return Yx(n,e,i,i.destroy),(e[Gi]??=new Ig).queries.push(new Cg(i))-1}function eD(n,e,t){let i=oi();return i.firstCreatePass&&(nD(i,new Ag(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),QA(i,lt(),e)}function tD(n){return n.split(",").map(e=>e.trim())}function nD(n,e,t){n.queries===null&&(n.queries=new Dg),n.queries.track(new Rg(e,t))}function av(n,e){return n.queries.getByIndex(e)}function iD(n,e){let t=n[Fe],i=av(t,e);return i.crossesNgTemplate?Ng(t,n,e,[]):db(t,n,i,e)}var Fo=class{},Ud=class{};var vd=class extends Fo{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new md(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let s=Em(e);this._bootstrapComponents=CS(s.bootstrap),this._r3Injector=Qm(e,t,[{provide:Fo,useValue:this},{provide:Sc,useValue:this.componentFactoryResolver},...i],Pu(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},yd=class extends Ud{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new vd(this.moduleType,e,[])}};var gc=class extends Fo{injector;componentFactoryResolver=new md(this);instance=null;constructor(e){super();let t=new Ts([...e.providers,{provide:Fo,useValue:this},{provide:Sc,useValue:this.componentFactoryResolver}],e.parent||tc(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function bc(n,e,t=null){return new gc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var rD=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Rm(!1,t.type),r=i.length>0?bc([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Ue({token:n,providedIn:"environment",factory:()=>new n(Xe(qt))})}return n})();function Hs(n){return wd(()=>{let e=fb(n),t=tt(oe({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===jg.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(rD).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||bi.Emulated,styles:n.styles||Hi,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&Vs("NgStandalone"),hb(t);let i=n.dependencies;return t.directiveDefs=H0(i,sD),t.pipeDefs=H0(i,Ax),t.id=cD(t),t})}function sD(n){return Fr(n)||wm(n)}function oD(n,e){if(n==null)return As;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=Dd.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function aD(n){if(n==null)return As;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function cv(n){return wd(()=>{let e=fb(n);return hb(e),e})}function fb(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||As,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Hi,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:oD(n.inputs,e),outputs:aD(n.outputs),debugInfo:null}}function hb(n){n.features?.forEach(e=>e(n))}function H0(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function cD(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function lD(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=Td(n.mergedAttrs,n.attrs);let u=n.tView=Yg(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Ro(n,!1);let c=uD(t,e,n,i);Qu()&&tv(t,e,c,n),Lo(c,e);let l=KS(c,e,c,n);e[i+un]=l,Kg(e,l),$A(l,n,e)}function _d(n,e,t,i,r,s,o,a,c,l,u){let d=t+un,f;if(e.firstCreatePass){if(f=Od(e,d,4,o||null,a||null),l!=null){let h=$i(e.consts,l);f.localNames=[];for(let m=0;m<h.length;m+=2)f.localNames.push(h[m],-1)}}else f=e.data[d];return lD(f,n,e,t,i,r,s,c),l!=null&&iv(n,f,u),f}var uD=dD;function dD(n,e,t,i){return ed(!0),e[Yt].createComment("")}var lv=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var uv=new Oe("");function Mc(n){return!!n&&typeof n.then=="function"}function pb(n){return!!n&&typeof n.subscribe=="function"}var mb=new Oe("");var dv=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=re(mb,{optional:!0})??[];injector=re(gi);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=yn(this.injector,r);if(Mc(s))t.push(s);else if(pb(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),kd=new Oe("");function gb(){Vp(()=>{let n="";throw new Ae(600,n)})}function vb(n){return n.isBoundToModule}var fD=10;var Ho=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=re(qi);afterRenderManager=re(NS);zonelessEnabled=re(oc);rootEffectScheduler=re(nd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new an;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=re(Gr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Ft(t=>!t))}constructor(){re(Bo,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=re(qt);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=gi.NULL){return this._injector.get(Vn).run(()=>{wt(ht.BootstrapComponentStart);let o=t instanceof Fd;if(!this._injector.get(dv).done){let m="";throw new Ae(405,m)}let c;o?c=t:c=this._injector.get(Sc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=vb(c)?void 0:this._injector.get(Fo),u=i||c.selector,d=c.create(r,[],u,l),f=d.location.nativeElement,h=d.injector.get(uv,null);return h?.registerApplication(f),d.onDestroy(()=>{this.detachView(d.hostView),lc(this.components,d),h?.unregisterApplication(f)}),this._loadComponent(d),wt(ht.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){wt(ht.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Qg.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw wt(ht.ChangeDetectionEnd),new Ae(101,!1);let t=De(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,De(t),this.afterTick.next(),wt(ht.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ks,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<fD;){wt(ht.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{wt(ht.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ic(r))continue;let s=i&&!this.zonelessEnabled?0:1;qS(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>ic(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;lc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(kd,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>lc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Ae(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function lc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}var Pg=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function ug(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function hD(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){De(i);let l=e.length-1;for(De(null);o<=a&&o<=l;){let u=n.at(o),d=e[o],f=ug(o,u,o,d,t);if(f!==0){f<0&&n.updateValue(o,d),o++;continue}let h=n.at(a),m=e[l],y=ug(a,h,l,m,t);if(y!==0){y<0&&n.updateValue(a,m),a--,l--;continue}let g=t(o,u),p=t(a,h),b=t(o,d);if(Object.is(b,p)){let A=t(l,m);Object.is(A,g)?(n.swap(o,a),n.updateValue(a,m),l--,a--):n.move(a,o),n.updateValue(o,d),o++;continue}if(r??=new xd,s??=G0(n,o,a,t),Lg(n,r,o,b))n.updateValue(o,d),o++,a++;else if(s.has(b))r.set(g,n.detach(o)),a--;else{let A=n.create(o,e[o]);n.attach(o,A),o++,a++}}for(;o<=l;)z0(n,r,t,o,e[o]),o++}else if(e!=null){De(i);let l=e[Symbol.iterator]();De(null);let u=l.next();for(;!u.done&&o<=a;){let d=n.at(o),f=u.value,h=ug(o,d,o,f,t);if(h!==0)h<0&&n.updateValue(o,f),o++,u=l.next();else{r??=new xd,s??=G0(n,o,a,t);let m=t(o,f);if(Lg(n,r,o,m))n.updateValue(o,f),o++,a++,u=l.next();else if(!s.has(m))n.attach(o,n.create(o,f)),o++,a++,u=l.next();else{let y=t(o,d);r.set(y,n.detach(o)),a--}}}for(;!u.done;)z0(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function Lg(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function z0(n,e,t,i,r){if(Lg(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function G0(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var xd=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function Mi(n,e,t,i,r,s,o,a){Vs("NgControlFlow");let c=lt(),l=oi(),u=$i(l.consts,s);return _d(c,l,n,e,t,i,r,u,256,o,a),fv}function fv(n,e,t,i,r,s,o,a){Vs("NgControlFlow");let c=lt(),l=oi(),u=$i(l.consts,s);return _d(c,l,n,e,t,i,r,u,512,o,a),fv}function Ei(n,e){Vs("NgControlFlow");let t=lt(),i=rc(),r=t[i]!==_n?t[i]:-1,s=r!==-1?Sd(t,un+r):void 0,o=0;if(jr(t,i,n)){let a=De(null);try{if(s!==void 0&&QS(s,o),n!==-1){let c=un+n,l=Sd(t,c),u=kg(t[Fe],c),d=tb(l,u,t),f=Pd(t,u,e,{dehydratedView:d});Ld(l,f,o,dc(u,d))}}finally{De(a)}}else if(s!==void 0){let a=JS(s,o);a!==void 0&&(a[Vt]=e)}}var Og=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Ut}};function hv(n,e){return e}var Fg=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function Ec(n,e,t,i,r,s,o,a,c,l,u,d,f){Vs("NgControlFlow");let h=lt(),m=oi(),y=c!==void 0,g=lt(),p=a?o.bind(g[zn][Vt]):o,b=new Fg(y,p);g[un+n]=b,_d(h,m,n+1,e,t,i,r,$i(m.consts,s),256),y&&_d(h,m,n+2,c,l,u,d,$i(m.consts,f),512)}var Ug=class extends Pg{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Ut}at(e){return this.getLView(e)[Vt].$implicit}attach(e,t){let i=t[Eo];this.needsIndexUpdate||=e!==this.length,Ld(this.lContainer,t,e,dc(this.templateTNode,i)),pD(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,mD(this.lContainer,e),gD(this.lContainer,e)}create(e,t){let i=bg(this.lContainer,this.templateTNode.tView.ssrId);return Pd(this.hostLView,this.templateTNode,new Og(this.lContainer,t,e),{dehydratedView:i})}destroy(e){Rd(e[Fe],e)}updateValue(e,t){this.getLView(e)[Vt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[Vt].$index=e}getLView(e){return vD(this.lContainer,e)}};function wc(n){let e=De(null),t=_i();try{let i=lt(),r=i[Fe],s=i[t],o=t+1,a=Sd(i,o);if(s.liveCollection===void 0){let l=kg(r,o);s.liveCollection=new Ug(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(hD(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=rc(),u=c.length===0;if(jr(i,l,u)){let d=t+2,f=Sd(i,d);if(u){let h=kg(r,d),m=tb(f,h,i),y=Pd(i,h,void 0,{dehydratedView:m});Ld(f,y,0,dc(h,m))}else r.firstUpdatePass&&dA(f),QS(f,0)}}}finally{De(e)}}function Sd(n,e){return n[e]}function pD(n,e){if(n.length<=Ut)return;let t=Ut+e,i=n[t],r=i?i[kr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[dr];_I(s,r),Us.delete(i[fr]),r.detachedLeaveAnimationFns=void 0}}function mD(n,e){if(n.length<=Ut)return;let t=Ut+e,i=n[t],r=i?i[kr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function gD(n,e){return hc(n,e)}function vD(n,e){return JS(n,e)}function kg(n,e){return ju(n,e)}function Bg(n,e,t,i,r){HS(e,n,t,r?"class":"style",i)}function bd(n,e,t,i){let r=lt(),s=r[Fe],o=n+un,a=s.firstCreatePass?ob(o,r,2,e,zI,Jx(),t,i):s.data[o];if(Hr(a)){let c=r[zi].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(cb(l),()=>(W0(n,e,r,a,i),bd))}}return W0(n,e,r,a,i),bd}function W0(n,e,t,i,r){if(BS(i,t,n,e,yb),Wu(i)){let s=t[Fe];kS(s,t,i),SS(s,i,t)}r!=null&&iv(t,i)}function pv(){let n=oi(),e=ai(),t=VS(e);return n.firstCreatePass&&ab(n,t),Wm(t)&&jm(),Gm(),t.classesWithoutHost!=null&&TC(t)&&Bg(n,t,lt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&CC(t)&&Bg(n,t,lt(),t.stylesWithoutHost,!1),pv}function zo(n,e,t,i){return bd(n,e,t,i),pv(),zo}function je(n,e,t,i){let r=lt(),s=r[Fe],o=n+un,a=s.firstCreatePass?wA(o,s,2,e,t,i):s.data[o];return BS(a,r,n,e,yb),i!=null&&iv(r,a),je}function ke(){let n=ai(),e=VS(n);return Wm(e)&&jm(),Gm(),ke}function Bd(n,e,t,i){return je(n,e,t,i),ke(),Bd}var yb=(n,e,t,i,r)=>(ed(!0),MS(e[Yt],i,Jm()));function Tc(){return lt()}function Go(n,e,t){let i=lt(),r=rc();if(jr(i,r,e)){let s=oi(),o=d0();kI(o,i,n,e,i[Yt],t)}return Go}var Cc="en-US";var yD=Cc;function _b(n){typeof n=="string"&&(yD=n.toLowerCase().replace(/_/g,"-"))}function Wn(n,e,t){let i=lt(),r=oi(),s=ai();return(s.type&3||t)&&AA(s,r,i,t,i[Yt],n,e,IA(s,i,e)),Wn}function dn(n=1){return u0(n)}function Vd(n,e,t){return eD(n,e,t),Vd}function Hd(n){let e=lt(),t=oi(),i=Ym();Zu(i+1);let r=av(t,i);if(n.dirty&&jx(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=iD(e,i);n.reset(s,HC),n.notifyOnChanges()}return!0}return!1}function zd(){return JA(lt(),Ym())}function id(n,e){return n<<17|e<<2}function Bs(n){return n>>17&32767}function _D(n){return(n&2)==2}function xD(n,e){return n&131071|e<<17}function Vg(n){return n|2}function Uo(n){return(n&131068)>>2}function dg(n,e){return n&-131069|e<<2}function SD(n){return(n&1)===1}function Hg(n){return n|1}function bD(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=Bs(o),c=Uo(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||bo(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=Bs(n[a+1]);n[i+1]=id(f,a),f!==0&&(n[f+1]=dg(n[f+1],i)),n[a+1]=xD(n[a+1],i)}else n[i+1]=id(a,0),a!==0&&(n[a+1]=dg(n[a+1],i)),a=i;else n[i+1]=id(c,0),a===0?a=i:n[c+1]=dg(n[c+1],i),c=i;l&&(n[i+1]=Vg(n[i+1])),j0(n,u,i,!0),j0(n,u,i,!1),MD(e,u,n,i,s),o=id(a,c),s?e.classBindings=o:e.styleBindings=o}function MD(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&bo(s,e)>=0&&(t[i+1]=Hg(t[i+1]))}function j0(n,e,t,i){let r=n[t+1],s=e===null,o=i?Bs(r):Uo(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];ED(c,e)&&(a=!0,n[o+1]=i?Hg(l):Vg(l)),o=i?Bs(l):Uo(l)}a&&(n[t+1]=i?Vg(r):Hg(r))}function ED(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?bo(n,e)>=0:!1}var Si={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function wD(n){return n.substring(Si.key,Si.keyEnd)}function TD(n){return CD(n),xb(n,Sb(n,0,Si.textEnd))}function xb(n,e){let t=Si.textEnd;return t===e?-1:(e=Si.keyEnd=ID(n,Si.key=e,t),Sb(n,e,t))}function CD(n){Si.key=0,Si.keyEnd=0,Si.value=0,Si.valueEnd=0,Si.textEnd=n.length}function Sb(n,e,t){for(;e<t&&n.charCodeAt(e)<=32;)e++;return e}function ID(n,e,t){for(;e<t&&n.charCodeAt(e)>32;)e++;return e}function mr(n){DD(UD,AD,n,!0)}function AD(n,e){for(let t=TD(e);t>=0;t=xb(e,t))Vu(n,wD(e),!0)}function DD(n,e,t,i){let r=oi(),s=sc(2);r.firstUpdatePass&&RD(r,null,s,i);let o=lt();if(t!==_n&&jr(o,s,t)){let a=r.data[_i()];if(Mb(a,i)&&!bb(r,s)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=Lu(c,t||"")),Bg(r,a,o,t,i)}else kD(r,a,o,o[Yt],o[s+1],o[s+1]=FD(n,e,t),i,s)}}function bb(n,e){return e>=n.expandoStartIndex}function RD(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[_i()],o=bb(n,t);Mb(s,i)&&e===null&&!o&&(e=!1),e=ND(r,s,e,i),bD(r,s,e,t,o,i)}}function ND(n,e,t,i){let r=o0(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=fg(null,n,e,t,i),t=vc(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=fg(r,n,e,t,i),s===null){let c=PD(n,e,i);c!==void 0&&Array.isArray(c)&&(c=fg(null,n,e,c[1],i),c=vc(c,e.attrs,i),LD(n,e,i,c))}else s=OD(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function PD(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Uo(i)!==0)return n[Bs(i)]}function LD(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[Bs(r)]=i}function OD(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=vc(i,o,t)}return vc(i,e.attrs,t)}function fg(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=vc(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function vc(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Vu(n,o,t?!0:e[++s]))}return n===void 0?null:n}function FD(n,e,t){if(t==null||t==="")return Hi;let i=[],r=bS(t);if(Array.isArray(r))for(let s=0;s<r.length;s++)n(i,r[s],!0);else if(r instanceof Set)for(let s of r)n(i,s,!0);else if(typeof r=="object")for(let s in r)r.hasOwnProperty(s)&&n(i,s,r[s]);else typeof r=="string"&&e(i,r);return i}function UD(n,e,t){let i=String(e);i!==""&&!i.includes(" ")&&Vu(n,i,t)}function kD(n,e,t,i,r,s,o,a){r===_n&&(r=Hi);let c=0,l=0,u=0<r.length?r[0]:null,d=0<s.length?s[0]:null;for(;u!==null||d!==null;){let f=c<r.length?r[c+1]:void 0,h=l<s.length?s[l+1]:void 0,m=null,y;u===d?(c+=2,l+=2,f!==h&&(m=d,y=h)):d===null||u!==null&&u<d?(c+=2,m=u):(l+=2,m=d,y=h),m!==null&&BD(n,e,t,i,m,y,o,a),u=c<r.length?r[c]:null,d=l<s.length?s[l]:null}}function BD(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=SD(l)?$0(c,e,t,r,Uo(l),o):void 0;if(!Md(u)){Md(s)||_D(l)&&(s=$0(c,null,t,r,a,o));let d=Um(_i(),t);LI(i,o,d,r,s)}}function $0(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===_n&&(f=d?Hi:void 0);let h=d?Hu(f,i):u===i?f:void 0;if(l&&!Md(h)&&(h=Hu(c,i)),Md(h)&&(a=h,o))return a;let m=n[r+1];r=o?Bs(m):Uo(m)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Hu(c,i))}return a}function Md(n){return n!==void 0}function Mb(n,e){return(n.flags&(e?8:16))!==0}function it(n,e=""){let t=lt(),i=oi(),r=n+un,s=i.firstCreatePass?Od(i,r,1,e,null):i.data[r],o=VD(i,t,s,e);t[r]=o,Qu()&&tv(i,t,o,s),Ro(s,!1)}var VD=(n,e,t,i)=>(ed(!0),JC(e[Yt],i));function HD(n,e,t,i=""){return jr(n,rc(),t)?e+Hn(t)+i:_n}function zD(n,e,t,i,r,s=""){let o=Xu(),a=mc(n,o,t,r);return sc(2),a?e+Hn(t)+i+Hn(r)+s:_n}function GD(n,e,t,i,r,s,o,a=""){let c=Xu(),l=TA(n,c,t,r,o);return sc(3),l?e+Hn(t)+i+Hn(r)+s+Hn(o)+a:_n}function WD(n,e,t,i,r,s,o,a,c,l,u,d,f,h=""){let m=Xu(),y=CA(n,m,t,r,o,c);return y=mc(n,m+4,u,f)||y,sc(6),y?e+Hn(t)+i+Hn(r)+s+Hn(o)+a+Hn(c)+l+Hn(u)+d+Hn(f)+h:_n}function wi(n){return zs("",n),wi}function zs(n,e,t){let i=lt(),r=HD(i,n,e,t);return r!==_n&&$d(i,_i(),r),zs}function Gd(n,e,t,i,r){let s=lt(),o=zD(s,n,e,t,i,r);return o!==_n&&$d(s,_i(),o),Gd}function Wd(n,e,t,i,r,s,o){let a=lt(),c=GD(a,n,e,t,i,r,s,o);return c!==_n&&$d(a,_i(),c),Wd}function jd(n,e,t,i,r,s,o,a,c,l,u,d,f){let h=lt(),m=WD(h,n,e,t,i,r,s,o,a,c,l,u,d,f);return m!==_n&&$d(h,_i(),m),jd}function $d(n,e,t){let i=Um(e,n);QC(n[Yt],i,t)}var Ed=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},mv=(()=>{class n{compileModuleSync(t){return new yd(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=Em(t),s=CS(r.declarations).reduce((o,a)=>{let c=Fr(a);return c&&o.push(new Oo(c)),o},[]);return new Ed(i,s)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Eb=(()=>{class n{applicationErrorHandler=re(qi);appRef=re(Ho);taskService=re(Gr);ngZone=re(Vn);zonelessEnabled=re(oc);tracing=re(Bo,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new on;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ya):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(re(rg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?m0:eg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ya+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function wb(){return[{provide:Cs,useExisting:Eb},{provide:Vn,useClass:Za},{provide:oc,useValue:!0}]}function jD(){return typeof $localize<"u"&&$localize.locale||Cc}var gv=new Oe("",{factory:()=>re(gv,{optional:!0,skipSelf:!0})||jD()});function Yi(n){return Ex(n)}function Ic(n,e){return iu(n,e?.equal)}var Db=Symbol("InputSignalNode#UNSET"),nR=tt(oe({},ru),{transformFn:void 0,applyValueToInputSignal(n,e){ho(n,e)}});function Rb(n,e){let t=Object.create(nR);t.value=n,t.transformFn=e?.transform;function i(){if(lo(t),t.value===Db){let r=null;throw new Ae(-950,r)}return t.value}return i[Cn]=t,i}function Tb(n,e){return Rb(n,e)}function iR(n){return Rb(Db,n)}var Nb=(Tb.required=iR,Tb);var vv=new Oe(""),rR=new Oe("");function Ac(n){return!n.moduleRef}function sR(n){let e=Ac(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Vn);return t.run(()=>{Ac(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(qi),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Ac(n)){let s=()=>e.destroy(),o=n.platformInjector.get(vv);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(vv);o.add(s),n.moduleRef.onDestroy(()=>{lc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return aR(i,t,()=>{let s=e.get(Gr),o=s.add(),a=e.get(dv);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(gv,Cc);if(_b(c||Cc),!e.get(rR,!0))return Ac(n)?e.get(Ho):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Ac(n)){let u=e.get(Ho);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return oR?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var oR;function aR(n,e,t){try{let i=t();return Mc(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var qd=null;function cR(n=[],e){return gi.create({name:e,providers:[{provide:ec,useValue:"platform"},{provide:vv,useValue:new Set([()=>qd=null])},...n]})}function lR(n=[]){if(qd)return qd;let e=cR(n);return qd=e,gb(),uR(e),e}function uR(n){let e=n.get(Id,null);yn(n,()=>{e?.forEach(t=>t())})}var dR=1e4;var L6=dR-1e3;var Pb=(()=>{class n{static __NG_ELEMENT_ID__=fR}return n})();function fR(n){return hR(ai(),lt(),(n&16)===16)}function hR(n,e,t){if(Hr(n)&&!t){let i=ji(n.index,e);return new Wr(i,i)}else if(n.type&175){let i=e[zn];return new Wr(i,e)}return null}function Lb(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;wt(ht.BootstrapApplicationStart);try{let s=r?.injector??lR(i),o=[wb(),v0,...t||[]],a=new gc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return sR({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{wt(ht.BootstrapApplicationEnd)}}var Ob=null;function gr(){return Ob}function yv(n){Ob??=n}var Dc=class{},Xd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(Fb),providedIn:"platform"})}return n})();var Fb=(()=>{class n extends Xd{_location;_history;_doc=re(Zt);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return gr().getBaseHref(this._doc)}onPopState(t){let i=gr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=gr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Bb(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function Ub(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function qr(n){return n&&n[0]!=="?"?`?${n}`:n}var Yd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(mR),providedIn:"root"})}return n})(),pR=new Oe(""),mR=(()=>{class n extends Yd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??re(Zt).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Bb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+qr(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,s){let o=this.prepareExternalUrl(r+qr(s));this._platformLocation.pushState(t,i,o)}replaceState(t,i,r,s){let o=this.prepareExternalUrl(r+qr(s));this._platformLocation.replaceState(t,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(Xe(Xd),Xe(pR,8))};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Wo=(()=>{class n{_subject=new an;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=yR(Ub(kb(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+qr(i))}normalize(t){return n.stripTrailingSlash(vR(this._basePath,kb(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+qr(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+qr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=qr;static joinWithSlash=Bb;static stripTrailingSlash=Ub;static \u0275fac=function(i){return new(i||n)(Xe(Yd))};static \u0275prov=Ue({token:n,factory:()=>gR(),providedIn:"root"})}return n})();function gR(){return new Wo(Xe(Yd))}function vR(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function kb(n){return n.replace(/\/index\.html$/,"")}function yR(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}function _v(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var Rc=class{};var Vb="browser";var Nc=class{_doc;constructor(e){this._doc=e}manager},Zd=(()=>{class n extends Nc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(Xe(Zt))};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})(),Qd=new Oe(""),Mv=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Zd));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Zd);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Ae(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(Xe(Qd),Xe(Vn))};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})(),xv="ng-app-id";function Hb(n){for(let e of n)e.remove()}function zb(n,e){let t=e.createElement("style");return t.textContent=n,t}function _R(n,e,t,i){let r=n.head?.querySelectorAll(`style[${xv}="${e}"],link[${xv}="${e}"]`);if(r)for(let s of r)s.removeAttribute(xv),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function bv(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Ev=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,_R(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,zb);i?.forEach(r=>this.addUsage(r,this.external,bv))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(Hb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Hb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,zb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,bv(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(Xe(Zt),Xe(Cd),Xe(Ad,8),Xe(xc))};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})(),Sv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},wv=/%COMP%/g;var Wb="%COMP%",xR=`_nghost-${Wb}`,SR=`_ngcontent-${Wb}`,bR=!0,MR=new Oe("",{factory:()=>bR});function ER(n){return SR.replace(wv,n)}function wR(n){return xR.replace(wv,n)}function jb(n,e){return e.map(t=>t.replace(wv,n))}var Tv=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Pc(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Jd?r.applyToHost(t):r instanceof Lc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case bi.Emulated:s=new Jd(c,l,i,this.appId,u,o,a,d);break;case bi.ShadowDom:return new Kd(c,t,i,o,a,this.nonce,d,l);case bi.ExperimentalIsolatedShadowDom:return new Kd(c,t,i,o,a,this.nonce,d);default:s=new Lc(c,l,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(Xe(Mv),Xe(Ev),Xe(Cd),Xe(MR),Xe(Zt),Xe(Vn),Xe(Ad),Xe(Bo,8))};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})(),Pc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(Sv[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Gb(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Gb(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Ae(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Sv[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Sv[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(pr.DashCase|pr.Important)?e.style.setProperty(t,i,r&pr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&pr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=gr().getGlobalEventTarget(this.doc,e),!e))throw new Ae(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function Gb(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Kd=class extends Pc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=jb(i.id,l);for(let d of l){let f=document.createElement("style");o&&f.setAttribute("nonce",o),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=bv(d,r);o&&f.setAttribute("nonce",o),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Lc=class extends Pc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?jb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Us.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Jd=class extends Lc{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=ER(l),this.hostAttr=wR(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var ef=class n extends Dc{supportsDOMEvents=!0;static makeCurrent(){yv(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=TR();return t==null?null:CR(t)}resetBaseElement(){Oc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return _v(document.cookie,e)}},Oc=null;function TR(){return Oc=Oc||document.head.querySelector("base"),Oc?Oc.getAttribute("href"):null}function CR(n){return new URL(n,document.baseURI).pathname}var IR=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})(),$b=["alt","control","meta","shift"],AR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},DR={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},qb=(()=>{class n extends Nc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>gr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),$b.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=AR[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),$b.forEach(o=>{if(o!==r){let a=DR[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(Xe(Zt))};static \u0275prov=Ue({token:n,factory:n.\u0275fac})}return n})();async function Cv(n,e,t){let i=oe({rootComponent:n},RR(e,t));return Lb(i)}function RR(n,e){return{platformRef:e?.platformRef,appProviders:[...FR,...n?.providers??[]],platformProviders:OR}}function NR(){ef.makeCurrent()}function PR(){return new ur}function LR(){return $g(document),document}var OR=[{provide:xc,useValue:Vb},{provide:Id,useValue:NR,multi:!0},{provide:Zt,useFactory:LR}];var FR=[{provide:ec,useValue:"root"},{provide:ur,useFactory:PR},{provide:Qd,useClass:Zd,multi:!0},{provide:Qd,useClass:qb,multi:!0},Tv,Ev,Mv,{provide:ks,useExisting:Tv},{provide:Rc,useClass:IR},[]];var Xb=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(Xe(Zt))};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var He="primary",Yc=Symbol("RouteTitle"),Pv=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function Ws(n){return new Pv(n)}function Iv(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(r[0]===":")t[r.substring(1)]=s;else if(r!==s.path)return!1}return!0}function nM(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return Iv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let s=i.slice(0,r),o=i.slice(r+1);if(s.length+o.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!Iv(s,n.slice(0,s.length),a)||!Iv(o,n.slice(n.length-o.length),a)?null:{consumed:n,posParams:a}}function af(n){return new Promise((e,t)=>{n.pipe(cr()).subscribe({next:i=>e(i),error:i=>t(i)})})}function kR(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!Ki(n[t],e[t]))return!1;return!0}function Ki(n,e){let t=n?Lv(n):void 0,i=e?Lv(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let s=0;s<t.length;s++)if(r=t[s],!iM(n[r],e[r]))return!1;return!0}function Lv(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function iM(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,s)=>i[s]===r)}else return n===e}function BR(n){return n.length>0?n[n.length-1]:null}function qs(n){return bu(n)?n:Mc(n)?Gt(Promise.resolve(n)):nt(n)}function rM(n){return bu(n)?af(n):Promise.resolve(n)}var VR={exact:aM,subset:cM},sM={exact:HR,subset:zR,ignored:()=>!0},oM={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Ov={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Yb(n,e,t){return VR[t.paths](n.root,e.root,t.matrixParams)&&sM[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function HR(n,e){return Ki(n,e)}function aM(n,e,t){if(!Gs(n.segments,e.segments)||!rf(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!aM(n.children[i],e.children[i],t))return!1;return!0}function zR(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>iM(n[t],e[t]))}function cM(n,e,t){return lM(n,e,e.segments,t)}function lM(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!Gs(r,t)||e.hasChildren()||!rf(r,t,i))}else if(n.segments.length===t.length){if(!Gs(n.segments,t)||!rf(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!cM(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),s=t.slice(n.segments.length);return!Gs(n.segments,r)||!rf(n.segments,r,i)||!n.children[He]?!1:lM(n.children[He],e,s,i)}}function rf(n,e,t){return e.every((i,r)=>sM[t](n[r].parameters,i.parameters))}var li=class{root;queryParams;fragment;_queryParamMap;constructor(e=new pt([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Ws(this.queryParams),this._queryParamMap}toString(){return jR.serialize(this)}},pt=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return sf(this)}},Xr=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=Ws(this.parameters),this._parameterMap}toString(){return dM(this)}};function GR(n,e){return Gs(n,e)&&n.every((t,i)=>Ki(t.parameters,e[i].parameters))}function Gs(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function WR(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===He&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==He&&(t=t.concat(e(r,i)))}),t}var Zc=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>new Yr,providedIn:"root"})}return n})(),Yr=class{parse(e){let t=new Uv(e);return new li(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${Fc(e.root,!0)}`,i=XR(e.queryParams),r=typeof e.fragment=="string"?`#${$R(e.fragment)}`:"";return`${t}${i}${r}`}},jR=new Yr;function sf(n){return n.segments.map(e=>dM(e)).join("/")}function Fc(n,e){if(!n.hasChildren())return sf(n);if(e){let t=n.children[He]?Fc(n.children[He],!1):"",i=[];return Object.entries(n.children).forEach(([r,s])=>{r!==He&&i.push(`${r}:${Fc(s,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=WR(n,(i,r)=>r===He?[Fc(n.children[He],!1)]:[`${r}:${Fc(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[He]!=null?`${sf(n)}/${t[0]}`:`${sf(n)}/(${t.join("//")})`}}function uM(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function tf(n){return uM(n).replace(/%3B/gi,";")}function $R(n){return encodeURI(n)}function Fv(n){return uM(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function of(n){return decodeURIComponent(n)}function Zb(n){return of(n.replace(/\+/g,"%20"))}function dM(n){return`${Fv(n.path)}${qR(n.parameters)}`}function qR(n){return Object.entries(n).map(([e,t])=>`;${Fv(e)}=${Fv(t)}`).join("")}function XR(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${tf(t)}=${tf(r)}`).join("&"):`${tf(t)}=${tf(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var Av=1073741824;function cf(n,e,t){Number(e)>=32&&!Object.hasOwn(n,Av)&&(n[Av]=t,delete n[Av]),n[e]=t}var YR=/^[^\/()?;#]+/;function Dv(n){let e=n.match(YR);return e?e[0]:""}var ZR=/^[^\/()?;=#]+/;function KR(n){let e=n.match(ZR);return e?e[0]:""}var JR=/^[^=?&#]+/;function QR(n){let e=n.match(JR);return e?e[0]:""}var eN=/^[^&#]+/;function tN(n){let e=n.match(eN);return e?e[0]:""}var Uv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new pt([],{}):new pt([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Ae(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[He]=new pt(t,i)),r}parseSegment(){let e=Dv(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Ae(4009,!1);return this.capture(e),new Xr(of(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=KR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Dv(this.remaining);r&&(i=r,this.capture(i))}cf(e,of(t),of(i))}parseQueryParam(e){let t=QR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let o=tN(this.remaining);o&&(i=o,this.capture(i))}let r=Zb(t),s=Zb(i);if(e.hasOwnProperty(r)){let o=e[r];Array.isArray(o)||(o=[o],e[r]=o),o.push(s)}else e[r]=s}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Dv(this.remaining),s=this.remaining[r.length];if(s!=="/"&&s!==")"&&s!==";")throw new Ae(4010,!1);let o;r.indexOf(":")>-1?(o=r.slice(0,r.indexOf(":")),this.capture(o),this.capture(":")):e&&(o=He);let a=this.parseChildren(t+1),c=Object.keys(a).length===1&&a[He]?a[He]:new pt([],a);cf(i,o??He,c),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Ae(4011,!1)}};function fM(n){return n.segments.length>0?new pt([],{[He]:n}):n}function hM(n){let e={};for(let[i,r]of Object.entries(n.children)){let s=hM(r);if(i===He&&s.segments.length===0&&s.hasChildren())for(let[o,a]of Object.entries(s.children))cf(e,o,a);else(s.segments.length>0||s.hasChildren())&&cf(e,i,s)}let t=new pt(n.segments,e);return nN(t)}function nN(n){if(n.numberOfChildren===1&&n.children[He]){let e=n.children[He];return new pt(n.segments.concat(e.segments),e.children)}return n}function Xo(n){return n instanceof li}function pM(n,e,t=null,i=null,r=new Yr){let s=mM(n);return gM(s,e,t,i,r)}function mM(n){let e;function t(s){let o={};for(let c of s.children){let l=t(c);o[c.outlet]=l}let a=new pt(s.url,o);return s===n&&(e=a),a}let i=t(n.root),r=fM(i);return e??r}function gM(n,e,t,i,r){let s=n;for(;s.parent;)s=s.parent;if(e.length===0)return Rv(s,s,s,t,i,r);let o=iN(e);if(o.toRoot())return Rv(s,s,new pt([],{}),t,i,r);let a=rN(o,s,n),c=a.processChildren?kc(a.segmentGroup,a.index,o.commands):yM(a.segmentGroup,a.index,o.commands);return Rv(s,a.segmentGroup,c,t,i,r)}function lf(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function Hc(n){return typeof n=="object"&&n!=null&&n.outlets}function Kb(n,e,t){n||="\u0275";let i=new li;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function Rv(n,e,t,i,r,s){let o={};for(let[l,u]of Object.entries(i??{}))o[l]=Array.isArray(u)?u.map(d=>Kb(l,d,s)):Kb(l,u,s);let a;n===e?a=t:a=vM(n,e,t);let c=fM(hM(a));return new li(c,o,r)}function vM(n,e,t){let i={};return Object.entries(n.children).forEach(([r,s])=>{s===e?i[r]=t:i[r]=vM(s,e,t)}),new pt(n.segments,i)}var uf=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&lf(i[0]))throw new Ae(4003,!1);let r=i.find(Hc);if(r&&r!==BR(i))throw new Ae(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function iN(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new uf(!0,0,n);let e=0,t=!1,i=n.reduce((r,s,o)=>{if(typeof s=="object"&&s!=null){if(s.outlets){let a={};return Object.entries(s.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(s.segmentPath)return[...r,s.segmentPath]}return typeof s!="string"?[...r,s]:o===0?(s.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,s]},[]);return new uf(t,e,i)}var $o=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function rN(n,e,t){if(n.isAbsolute)return new $o(e,!0,0);if(!t)return new $o(e,!1,NaN);if(t.parent===null)return new $o(t,!0,0);let i=lf(n.commands[0])?0:1,r=t.segments.length-1+i;return sN(t,r,n.numberOfDoubleDots)}function sN(n,e,t){let i=n,r=e,s=t;for(;s>r;){if(s-=r,i=i.parent,!i)throw new Ae(4005,!1);r=i.segments.length}return new $o(i,!1,r-s)}function oN(n){return Hc(n[0])?n[0].outlets:{[He]:n}}function yM(n,e,t){if(n??=new pt([],{}),n.segments.length===0&&n.hasChildren())return kc(n,e,t);let i=aN(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let s=new pt(n.segments.slice(0,i.pathIndex),{});return s.children[He]=new pt(n.segments.slice(i.pathIndex),n.children),kc(s,0,r)}else return i.match&&r.length===0?new pt(n.segments,{}):i.match&&!n.hasChildren()?kv(n,e,t):i.match?kc(n,0,r):kv(n,e,t)}function kc(n,e,t){if(t.length===0)return new pt(n.segments,{});{let i=oN(t),r={};if(Object.keys(i).some(s=>s!==He)&&n.children[He]&&n.numberOfChildren===1&&n.children[He].segments.length===0){let s=kc(n.children[He],e,t);return new pt(n.segments,s.children)}return Object.entries(i).forEach(([s,o])=>{typeof o=="string"&&(o=[o]),o!==null&&(r[s]=yM(n.children[s],e,o))}),Object.entries(n.children).forEach(([s,o])=>{i[s]===void 0&&(r[s]=o)}),new pt(n.segments,r)}}function aN(n,e,t){let i=0,r=e,s={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return s;let o=n.segments[r],a=t[i];if(Hc(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!Qb(c,l,o))return s;i+=2}else{if(!Qb(c,{},o))return s;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function kv(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let s=t[r];if(Hc(s)){let c=cN(s.outlets);return new pt(i,c)}if(r===0&&lf(t[0])){let c=n.segments[e];i.push(new Xr(c.path,Jb(t[0]))),r++;continue}let o=Hc(s)?s.outlets[He]:`${s}`,a=r<t.length-1?t[r+1]:null;o&&a&&lf(a)?(i.push(new Xr(o,Jb(a))),r+=2):(i.push(new Xr(o,{})),r++)}return new pt(i,{})}function cN(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=kv(new pt([],{}),0,i))}),e}function Jb(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function Qb(n,e,t){return n==t.path&&Ki(e,t.parameters)}var Bc="imperative",nn=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(nn||{}),jn=class{id;url;constructor(e,t){this.id=e,this.url=t}},js=class extends jn{type=nn.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},vr=class extends jn{urlAfterRedirects;type=nn.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},xn=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(xn||{}),zc=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(zc||{}),ci=class extends jn{reason;code;type=nn.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function _M(n){return n instanceof ci&&(n.code===xn.Redirect||n.code===xn.SupersededByNewNavigation)}var yr=class extends jn{reason;code;type=nn.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},$s=class extends jn{error;target;type=nn.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Gc=class extends jn{urlAfterRedirects;state;type=nn.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},df=class extends jn{urlAfterRedirects;state;type=nn.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ff=class extends jn{urlAfterRedirects;state;shouldActivate;type=nn.GuardsCheckEnd;constructor(e,t,i,r,s){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=s}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},hf=class extends jn{urlAfterRedirects;state;type=nn.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},pf=class extends jn{urlAfterRedirects;state;type=nn.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},mf=class{route;type=nn.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},gf=class{route;type=nn.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},vf=class{snapshot;type=nn.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},yf=class{snapshot;type=nn.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},_f=class{snapshot;type=nn.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},xf=class{snapshot;type=nn.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Yo=class{},Wc=class{},Zo=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function lN(n){return!(n instanceof Yo)&&!(n instanceof Zo)&&!(n instanceof Wc)}var Sf=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new ea(this.rootInjector)}},ea=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new Sf(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(Xe(qt))};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),bf=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=Bv(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=Bv(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=Vv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return Vv(e,this._root).map(t=>t.value)}};function Bv(n,e){if(n===e.value)return e;for(let t of e.children){let i=Bv(n,t);if(i)return i}return null}function Vv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=Vv(n,t);if(i.length)return i.unshift(e),i}return[]}var Rn=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function jo(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var jc=class extends bf{snapshot;constructor(e,t){super(e),this.snapshot=t,Yv(this,e)}toString(){return this.snapshot.toString()}};function xM(n,e){let t=uN(n,e),i=new cn([new Xr("",{})]),r=new cn({}),s=new cn({}),o=new cn({}),a=new cn(""),c=new Zr(i,r,o,a,s,He,n,t.root);return c.snapshot=t.root,new jc(new Rn(c,[]),t)}function uN(n,e){let t={},i={},r={},o=new Ko([],t,r,"",i,He,n,null,{},e);return new $c("",new Rn(o,[]))}var Zr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,s,o,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=s,this.outlet=o,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Ft(l=>l[Yc]))??nt(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Ft(e=>Ws(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Ft(e=>Ws(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Xv(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:Object.keys(n.params).length===0?e.params:Object.freeze(oe(oe({},e.params),n.params)),data:Object.freeze(oe(oe({},e.data),n.data)),resolve:oe(oe(oe(oe({},n.data),e.data),r?.data),n._resolvedData)}:i={params:Object.freeze(oe({},n.params)),data:Object.freeze(oe({},n.data)),resolve:oe(oe({},n.data),n._resolvedData??{})},r&&bM(r)&&(i.resolve[Yc]=r.title),i}var Ko=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Yc]}constructor(e,t,i,r,s,o,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s,this.outlet=o,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Ws(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Ws(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},$c=class extends bf{url;constructor(e,t){super(t),this.url=e,Yv(this,t)}toString(){return SM(this._root)}};function Yv(n,e){e.value._routerState=n,e.children.forEach(t=>Yv(n,t))}function SM(n){let e=n.children.length>0?` { ${n.children.map(SM).join(", ")} } `:"";return`${n.value}${e}`}function Nv(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,Ki(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),Ki(e.params,t.params)||n.paramsSubject.next(t.params),kR(e.url,t.url)||n.urlSubject.next(t.url),Ki(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function Hv(n,e){let t=Ki(n.params,e.params)&&GR(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||Hv(n.parent,e.parent))}function bM(n){return typeof n.title=="string"||n.title===null}var MM=new Oe(""),Kc=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=He;activateEvents=new An;deactivateEvents=new An;attachEvents=new An;detachEvents=new An;routerOutletData=Nb();parentContexts=re(ea);location=re(Vo);changeDetector=re(Pb);inputBinder=re(Tf,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Ae(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Ae(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Ae(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Ae(4013,!1);this._activatedRoute=t;let r=this.location,o=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new zv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(o,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=cv({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Gg]})}return n})(),zv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===Zr?this.route:e===ea?this.childContexts:e===MM?this.outletData:this.parent.get(e,t)}},Tf=new Oe("");var Zv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=Hs({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&zo(0,"router-outlet")},dependencies:[Kc],encapsulation:2})}return n})();function Kv(n){let e=n.children&&n.children.map(Kv),t=e?tt(oe({},n),{children:e}):oe({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==He&&(t.component=Zv),t}function dN(n,e,t){let i=qc(n,e._root,t?t._root:void 0);return new jc(i,e)}function qc(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=fN(n,e,t);return new Rn(i,r)}else{if(n.shouldAttach(e.value)){let s=n.retrieve(e.value);if(s!==null){let o=s.route;return o.value._futureSnapshot=e.value,o.children=e.children.map(a=>qc(n,a)),o}}let i=hN(e.value),r=e.children.map(s=>qc(n,s));return new Rn(i,r)}}function fN(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return qc(n,i,r);return qc(n,i)})}function hN(n){return new Zr(new cn(n.url),new cn(n.params),new cn(n.queryParams),new cn(n.fragment),new cn(n.data),n.outlet,n.component,n)}var Jo=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},EM="ngNavigationCancelingError";function Mf(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=Xo(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=wM(!1,xn.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function wM(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[EM]=!0,t.cancellationCode=e,t}function pN(n){return TM(n)&&Xo(n.url)}function TM(n){return!!n&&n[EM]}var Gv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,s){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=s}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),Nv(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=jo(t);e.children.forEach(s=>{let o=s.value.outlet;this.deactivateRoutes(s,r[o],i),delete r[o]}),Object.values(r).forEach(s=>{this.deactivateRouteAndItsChildren(s,i)})}deactivateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(r===s)if(r.component){let o=i.getContext(r.outlet);o&&this.deactivateChildRoutes(e,t,o.children)}else this.deactivateChildRoutes(e,t,i);else s&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=jo(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);if(i&&i.outlet){let o=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:o,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=jo(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=jo(t);e.children.forEach(s=>{this.activateRoutes(s,r[s.value.outlet],i),this.forwardEvent(new xf(s.value.snapshot))}),e.children.length&&this.forwardEvent(new yf(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(Nv(r),r===s)if(r.component){let o=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,o.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let o=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),o.children.onOutletReAttached(a.contexts),o.attachRef=a.componentRef,o.route=a.route.value,o.outlet&&o.outlet.attach(a.componentRef,a.route.value),Nv(a.route.value),this.activateChildRoutes(e,null,o.children)}else o.attachRef=null,o.route=r,o.outlet&&o.outlet.activateWith(r,o.injector),this.activateChildRoutes(e,null,o.children)}else this.activateChildRoutes(e,null,i)}},Ef=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},qo=class{component;route;constructor(e,t){this.component=e,this.route=t}};function mN(n,e,t){let i=n._root,r=e?e._root:null;return Uc(i,r,t,[i.value])}function gN(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function ta(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!ym(n)?n:e.get(n):i}function Uc(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=jo(e);return n.children.forEach(o=>{vN(o,s[o.value.outlet],t,i.concat([o.value]),r),delete s[o.value.outlet]}),Object.entries(s).forEach(([o,a])=>Vc(a,t.getContext(o),r)),r}function vN(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=n.value,o=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(o&&s.routeConfig===o.routeConfig){let c=yN(o,s,s.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Ef(i)):(s.data=o.data,s._resolvedData=o._resolvedData),s.component?Uc(n,e,a?a.children:null,i,r):Uc(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new qo(a.outlet.component,o))}else o&&Vc(e,a,r),r.canActivateChecks.push(new Ef(i)),s.component?Uc(n,null,a?a.children:null,i,r):Uc(n,null,t,i,r);return r}function yN(n,e,t){if(typeof t=="function")return yn(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!Gs(n.url,e.url);case"pathParamsOrQueryParamsChange":return!Gs(n.url,e.url)||!Ki(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Hv(n,e)||!Ki(n.queryParams,e.queryParams);default:return!Hv(n,e)}}function Vc(n,e,t){let i=jo(n),r=n.value;Object.entries(i).forEach(([s,o])=>{r.component?e?Vc(o,e.children.getContext(s),t):Vc(o,null,t):Vc(o,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new qo(e.outlet.component,r)):t.canDeactivateChecks.push(new qo(null,r)):t.canDeactivateChecks.push(new qo(null,r))}function Jc(n){return typeof n=="function"}function _N(n){return typeof n=="boolean"}function xN(n){return n&&Jc(n.canLoad)}function SN(n){return n&&Jc(n.canActivate)}function bN(n){return n&&Jc(n.canActivateChild)}function MN(n){return n&&Jc(n.canDeactivate)}function EN(n){return n&&Jc(n.canMatch)}function CM(n){return n instanceof bs||n?.name==="EmptyError"}var nf=Symbol("INITIAL_VALUE");function Qo(){return mi(n=>em(n.map(e=>e.pipe(ar(1),nm(nf)))).pipe(Ft(e=>{for(let t of e)if(t!==!0){if(t===nf)return nf;if(t===!1||wN(t))return t}return!0}),or(e=>e!==nf),ar(1)))}function wN(n){return Xo(n)||n instanceof Jo}function IM(n){return n.aborted?nt(void 0).pipe(ar(1)):new ft(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function AM(n){return Wa(IM(n))}function TN(n){return In(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:s}}=e;return s.length===0&&r.length===0?nt(tt(oe({},e),{guardsResult:!0})):CN(s,t,i).pipe(In(o=>o&&_N(o)?IN(t,r,n):nt(o)),Ft(o=>tt(oe({},e),{guardsResult:o})))})}function CN(n,e,t){return Gt(n).pipe(In(i=>PN(i.component,i.route,t,e)),cr(i=>i!==!0,!0))}function IN(n,e,t){return Gt(e).pipe(Mu(i=>_o(DN(i.route.parent,t),AN(i.route,t),NN(n,i.path),RN(n,i.route))),cr(i=>i!==!0,!0))}function AN(n,e){return n!==null&&e&&e(new _f(n)),nt(!0)}function DN(n,e){return n!==null&&e&&e(new vf(n)),nt(!0)}function RN(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return nt(!0);let i=t.map(r=>za(()=>{let s=e._environmentInjector,o=ta(r,s),a=SN(o)?o.canActivate(e,n):yn(s,()=>o(e,n));return qs(a).pipe(cr())}));return nt(i).pipe(Qo())}function NN(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(s=>gN(s)).filter(s=>s!==null).map(s=>za(()=>{let o=s.guards.map(a=>{let c=s.node._environmentInjector,l=ta(a,c),u=bN(l)?l.canActivateChild(t,n):yn(c,()=>l(t,n));return qs(u).pipe(cr())});return nt(o).pipe(Qo())}));return nt(r).pipe(Qo())}function PN(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return nt(!0);let s=r.map(o=>{let a=e._environmentInjector,c=ta(o,a),l=MN(c)?c.canDeactivate(n,e,t,i):yn(a,()=>c(n,e,t,i));return qs(l).pipe(cr())});return nt(s).pipe(Qo())}function LN(n,e,t,i,r){let s=e.canLoad;if(s===void 0||s.length===0)return nt(!0);let o=s.map(a=>{let c=ta(a,n),l=xN(c)?c.canLoad(e,t):yn(n,()=>c(e,t)),u=qs(l);return r?u.pipe(AM(r)):u});return nt(o).pipe(Qo(),DM(i))}function DM(n){return Zp(ni(e=>{if(typeof e!="boolean")throw Mf(n,e)}),Ft(e=>e===!0))}function ON(n,e,t,i,r,s){let o=e.canMatch;if(!o||o.length===0)return nt(!0);let a=o.map(c=>{let l=ta(c,n),u=EN(l)?l.canMatch(e,t,r):yn(n,()=>l(e,t,r));return qs(u).pipe(AM(s))});return nt(a).pipe(Qo(),DM(i))}var Zi=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},Xc=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function FN(n){throw new Ae(4e3,!1)}function UN(n){throw wM(!1,xn.GuardRejected)}var Wv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}async lineralizeSegments(e,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[He])throw FN(`${e.redirectTo}`);r=r.children[He]}}async applyRedirectCommands(e,t,i,r,s){let o=await kN(t,r,s);if(o instanceof li)throw new Xc(o);let a=this.applyRedirectCreateUrlTree(o,this.urlSerializer.parse(o),e,i);if(o[0]==="/")throw new Xc(a);return a}applyRedirectCreateUrlTree(e,t,i,r){let s=this.createSegmentGroup(e,t.root,i,r);return new li(s,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,s])=>{if(typeof s=="string"&&s[0]===":"){let a=s.substring(1);i[r]=t[a]}else i[r]=s}),i}createSegmentGroup(e,t,i,r){let s=this.createSegments(e,t.segments,i,r),o={};return Object.entries(t.children).forEach(([a,c])=>{o[a]=this.createSegmentGroup(e,c,i,r)}),new pt(s,o)}createSegments(e,t,i,r){return t.map(s=>s.path[0]===":"?this.findPosParam(e,s,r):this.findOrReturn(s,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Ae(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function kN(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n;return af(qs(yn(t,()=>i(e))))}function BN(n,e){return n.providers&&!n._injector&&(n._injector=bc(n.providers,e,`Route: ${n.path}`)),n._injector??e}function Ti(n){return n.outlet||He}function VN(n,e){let t=n.filter(i=>Ti(i)===e);return t.push(...n.filter(i=>Ti(i)!==e)),t}var jv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function RM(n){return{routeConfig:n.routeConfig,url:n.url,params:n.params,queryParams:n.queryParams,fragment:n.fragment,data:n.data,outlet:n.outlet,title:n.title,paramMap:n.paramMap,queryParamMap:n.queryParamMap}}function HN(n,e,t,i,r,s,o){let a=NM(n,e,t);if(!a.matched)return nt(a);let c=RM(s(a));return i=BN(e,i),ON(i,e,t,r,c,o).pipe(Ft(l=>l===!0?a:oe({},jv)))}function NM(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?oe({},jv):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||nM)(t,n,e);if(!r)return oe({},jv);let s={};Object.entries(r.posParams??{}).forEach(([a,c])=>{s[a]=c.path});let o=r.consumed.length>0?oe(oe({},s),r.consumed[r.consumed.length-1].parameters):s;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:o,positionalParamSegments:r.posParams??{}}}function eM(n,e,t,i,r){return t.length>0&&WN(n,t,i,r)?{segmentGroup:new pt(e,GN(i,new pt(t,n.children))),slicedSegments:[]}:t.length===0&&jN(n,t,i)?{segmentGroup:new pt(n.segments,zN(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new pt(n.segments,n.children),slicedSegments:t}}function zN(n,e,t,i){let r={};for(let s of t)if(Cf(n,e,s)&&!i[Ti(s)]){let o=new pt([],{});r[Ti(s)]=o}return oe(oe({},i),r)}function GN(n,e){let t={};t[He]=e;for(let i of n)if(i.path===""&&Ti(i)!==He){let r=new pt([],{});t[Ti(i)]=r}return t}function WN(n,e,t,i){return t.some(r=>!Cf(n,e,r)||!(Ti(r)!==He)?!1:!(i!==void 0&&Ti(r)===i))}function jN(n,e,t){return t.some(i=>Cf(n,e,i))}function Cf(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function $N(n,e,t){return e.length===0&&!n.hasChildren()}var $v=class{};async function qN(n,e,t,i,r,s,o="emptyOnly",a){return new qv(n,e,t,i,r,o,s,a).recognize()}var XN=31,qv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;queryParams;constructor(e,t,i,r,s,o,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=s,this.paramsInheritanceStrategy=o,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Wv(this.urlSerializer,this.urlTree),this.queryParams=Object.freeze(oe({},this.urlTree.queryParams))}noMatchError(e){return new Ae(4002,`'${e.segmentGroup}'`)}async recognize(){let e=eM(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(e),r=new Rn(i,t),s=new $c("",r),o=pM(i,[],this.urlTree.queryParams,this.urlTree.fragment);return o.queryParams=this.urlTree.queryParams,s.url=this.urlSerializer.serialize(o),{state:s,tree:o}}async match(e){let t=new Ko([],Object.freeze({}),this.queryParams,this.urlTree.fragment,Object.freeze({}),He,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,e,He,t),rootSnapshot:t}}catch(i){if(i instanceof Xc)return this.urlTree=i.urlTree,this.queryParams=Object.freeze(oe({},this.urlTree.queryParams)),this.match(i.urlTree.root);throw i instanceof Zi?this.noMatchError(i):i}}async processSegmentGroup(e,t,i,r,s){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,s);let o=await this.processSegment(e,t,i,i.segments,r,!0,s);return o instanceof Rn?[o]:[]}async processChildren(e,t,i,r){let s=[];for(let c of Object.keys(i.children))c==="primary"?s.unshift(c):s.push(c);let o=[];for(let c of s){let l=i.children[c],u=VN(t,c),d=await this.processSegment(e,u,l,l.segments,c,!0,r);d instanceof Rn&&o.push(d)}let a=PM(o);return YN(a),a}async processSegment(e,t,i,r,s,o,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,s,o,a)}catch(l){if(l instanceof Zi||CM(l))continue;throw l}if($N(i,r))return new $v;throw new Zi(i)}async processSegmentAgainstRoute(e,t,i,r,s,o,a,c){if(Ti(i)!==o&&(o===He||!Cf(r,s,i)||!i.children?.length&&!i.loadChildren||s.length===0&&!r.hasChildren()))throw new Zi(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,s,o,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,s,o,c);throw new Zi(r)}async expandSegmentAgainstRouteUsingRedirect(e,t,i,r,s,o,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=NM(t,r,s);if(!c)throw new Zi(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>XN&&(this.allowRedirects=!1));let h=this.createSnapshot(e,r,s,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let m=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,RM(h),e),y=await this.applyRedirects.lineralizeSegments(r,m);return this.processSegment(e,i,t,y.concat(f),o,!1,a)}createSnapshot(e,t,i,r,s){let o=new Ko(i,r,this.queryParams,this.urlTree.fragment,JN(t),Ti(t),t.component??t._loadedComponent??null,t,QN(t),e),a=Xv(o,s,this.paramsInheritanceStrategy);return o.params=a.params,o.data=a.data,o}async matchSegmentAgainstRoute(e,t,i,r,s,o){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=A=>this.createSnapshot(e,i,A.consumedSegments,A.parameters,o),c=await af(HN(t,i,r,e,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new Zi(t);e=i._injector??e;let{routes:l}=await this.getChildConfig(e,i,r),u=i._loadedInjector??e,{parameters:d,consumedSegments:f,remainingSegments:h}=c,m=this.createSnapshot(e,i,f,d,o),{segmentGroup:y,slicedSegments:g}=eM(t,f,h,l,s),p=Ti(i)===s;if(p&&g.length===0&&y.hasChildren()){let A=await this.processChildren(u,l,y,m);return new Rn(m,A)}if(p&&l.length===0&&g.length===0)return new Rn(m,[]);let b=await this.processSegment(u,l,y,g,p?He:s,!0,m);if(!p&&!(b instanceof Rn))throw new Zi(t);return new Rn(m,b instanceof Rn?[b]:[])}async getChildConfig(e,t,i){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let s=t._loadedNgModuleFactory;return s&&!t._loadedInjector&&(t._loadedInjector=s.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await af(LN(e,t,i,this.urlSerializer,this.abortSignal))){let s=await this.configLoader.loadChildren(e,t);return t._loadedRoutes=s.routes,t._loadedInjector=s.injector,t._loadedNgModuleFactory=s.factory,s}throw UN(t)}return{routes:[],injector:e}}};function YN(n){n.sort((e,t)=>e.value.outlet===He?-1:t.value.outlet===He?1:e.value.outlet.localeCompare(t.value.outlet))}function ZN(n){let e=n.value.routeConfig;return e&&e.path===""}function PM(n){let e=[],t=new Set;for(let r of n){if(!ZN(r)){e.push(r);continue}let s=e.find(o=>r.value.routeConfig===o.value.routeConfig);s!==void 0?(s.children.push(...r.children),t.add(s)):e.push(r)}for(let r of t){let s=PM(r.children);e.push(new Rn(r.value,s))}let i=e.filter(r=>!t.has(r));return KN(i),i}function KN(n){let e=Object.create(null);n.forEach(t=>{let i=e[t.value.outlet];if(i){let r=i.url.map(o=>o.toString()).join("/"),s=t.value.url.map(o=>o.toString()).join("/");throw new Ae(4006,!1)}e[t.value.outlet]=t.value})}function JN(n){return n.data||{}}function QN(n){return n.resolve||{}}function e1(n,e,t,i,r,s,o){return In(async a=>{let{state:c,tree:l}=await qN(n,e,t,i,a.extractedUrl,r,s,o);return tt(oe({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function t1(n){return In(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return nt(e);let r=new Set(i.map(a=>a.route)),s=new Set;for(let a of r)if(!s.has(a))for(let c of LM(a))s.add(c);let o=0;return Gt(s).pipe(Mu(a=>r.has(a)?n1(a,t,n):(a.data=Xv(a,a.parent,n).resolve,nt(void 0))),ni(()=>o++),Eu(1),In(a=>o===s.size?nt(e):ln))})}function LM(n){let e=n.children.map(t=>LM(t)).flat();return[n,...e]}function n1(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!bM(i)&&(r[Yc]=i.title),za(()=>(n.data=Xv(n,n.parent,t).resolve,i1(r,n,e).pipe(Ft(s=>(n._resolvedData=s,n.data=oe(oe({},n.data),s),null)))))}function i1(n,e,t){let i=Lv(n);if(i.length===0)return nt({});let r={};return Gt(i).pipe(In(s=>r1(n[s],e,t).pipe(cr(),ni(o=>{if(o instanceof Jo)throw Mf(new Yr,o);r[s]=o}))),Eu(1),Ft(()=>r),Ga(s=>CM(s)?ln:Qp(s)))}function r1(n,e,t){let i=e._environmentInjector,r=ta(n,i),s=r.resolve?r.resolve(e,t):yn(i,()=>r(e,t));return qs(s)}function tM(n){return mi(e=>{let t=n(e);return t?Gt(t).pipe(Ft(()=>e)):nt(e)})}var Jv=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(s=>s.outlet===He);return i}getResolvedTitleForRoute(t){return t.data[Yc]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(OM),providedIn:"root"})}return n})(),OM=(()=>{class n extends Jv{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(Xe(Xb))};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Qc=new Oe("",{factory:()=>({})}),el=new Oe(""),FM=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=re(mv);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let s=await rM(yn(t,()=>i.loadComponent())),o=await BM(kM(s));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=o,o}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let s=await UM(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,s}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();async function UM(n,e,t,i){let r=await rM(yn(t,()=>n.loadChildren())),s=await BM(kM(r)),o;s instanceof Ud||Array.isArray(s)?o=s:o=await e.compileModuleAsync(s),i&&i(n);let a,c,l=!1,u;return Array.isArray(o)?(c=o,l=!0):(a=o.create(t).injector,u=o,c=a.get(el,[],{optional:!0,self:!0}).flat()),{routes:c.map(Kv),injector:a,factory:u}}function s1(n){return n&&typeof n=="object"&&"default"in n}function kM(n){return s1(n)?n.default:n}async function BM(n){return n}var If=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(o1),providedIn:"root"})}return n})(),o1=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),VM=new Oe("");var a1=()=>{},HM=new Oe(""),zM=(()=>{class n{currentNavigation=Dn(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Dn(null);events=new an;transitionAbortWithErrorSubject=new an;configLoader=re(FM);environmentInjector=re(qt);destroyRef=re(hr);urlSerializer=re(Zc);rootContexts=re(ea);location=re(Wo);inputBindingEnabled=re(Tf,{optional:!0})!==null;titleStrategy=re(Jv);options=re(Qc,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=re(If);createViewTransition=re(VM,{optional:!0});navigationErrorHandler=re(HM,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>nt(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new mf(r)),i=r=>this.events.next(new gf(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;Yi(()=>{this.transitions?.next(tt(oe({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new cn(null),this.transitions.pipe(or(i=>i!==null),mi(i=>{let r=!1,s=new AbortController,o=()=>!r&&this.currentTransition?.id===i.id;return nt(i).pipe(mi(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",xn.SupersededByNewNavigation),ln;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?tt(oe({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new yr(a.id,this.urlSerializer.serialize(a.rawUrl),"",zc.IgnoredSameUrlNavigation)),a.resolve(!1),ln;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return nt(a).pipe(mi(d=>(this.events.next(new js(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?ln:Promise.resolve(d))),e1(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),ni(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(f=>(f.finalUrl=d.urlAfterRedirects,f)),this.events.next(new Wc)}),mi(d=>Gt(i.routesRecognizeHandler.deferredHandle??nt(void 0)).pipe(Ft(()=>d))),ni(()=>{let d=new Gc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:m,extras:y}=a,g=new js(d,this.urlSerializer.serialize(f),h,m);this.events.next(g);let p=xM(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=tt(oe({},a),{targetSnapshot:p,urlAfterRedirects:f,extras:tt(oe({},y),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(b=>(b.finalUrl=f,b)),nt(i)}else return this.events.next(new yr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",zc.IgnoredByUrlHandlingStrategy)),a.resolve(!1),ln}),Ft(a=>{let c=new df(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=tt(oe({},a),{guards:mN(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),TN(a=>this.events.next(a)),mi(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw Mf(this.urlSerializer,a.guardsResult);let c=new ff(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!o())return ln;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",xn.GuardRejected),ln;if(a.guards.canActivateChecks.length===0)return nt(a);let l=new hf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!o())return ln;let u=!1;return nt(a).pipe(t1(this.paramsInheritanceStrategy),ni({next:()=>{u=!0;let d=new pf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",xn.NoDataFromResolver)}}))}),tM(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let f=u._environmentInjector;d.push(this.configLoader.loadComponent(f,u.routeConfig).then(h=>{u.component=h}))}for(let f of u.children)d.push(...c(f));return d},l=c(a.targetSnapshot.root);return l.length===0?nt(a):Gt(Promise.all(l).then(()=>a))}),tM(()=>this.afterPreactivation()),mi(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?Gt(l).pipe(Ft(()=>i)):nt(i)}),ar(1),mi(a=>{let c=dN(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=tt(oe({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new Yo);let l=i.beforeActivateHandler.deferredHandle;return l?Gt(l.then(()=>a)):nt(a)}),ni(a=>{new Gv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),o()&&(r=!0,this.currentNavigation.update(c=>(c.abort=a1,c)),this.lastSuccessfulNavigation.set(Yi(this.currentNavigation)),this.events.next(new vr(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Wa(IM(s.signal).pipe(or(()=>!r&&!i.targetRouterState),ni(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",xn.Aborted)}))),ni({complete:()=>{r=!0}}),Wa(this.transitionAbortWithErrorSubject.pipe(ni(a=>{throw a}))),tm(()=>{s.abort(),r||this.cancelNavigationTransition(i,"",xn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Ga(a=>{if(r=!0,this.destroyed)return i.resolve(!1),ln;if(TM(a))this.events.next(new ci(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),pN(a)?this.events.next(new Zo(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new $s(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=yn(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof Jo){let{message:u,cancellationCode:d}=Mf(this.urlSerializer,l);this.events.next(new ci(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new Zo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return ln}))}))}cancelNavigationTransition(t,i,r){let s=new ci(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(s),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Yi(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function c1(n){return n!==Bc}var GM=new Oe("");var WM=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(l1),providedIn:"root"})}return n})(),wf=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},l1=(()=>{class n extends wf{static \u0275fac=(()=>{let t;return function(r){return(t||(t=yc(n)))(r||n)}})();static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Qv=(()=>{class n{urlSerializer=re(Zc);options=re(Qc,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=re(Wo);urlHandlingStrategy=re(If);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new li;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let s=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,o=r??s;return o instanceof li?this.urlSerializer.serialize(o):o}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=xM(null,re(qt));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:()=>re(u1),providedIn:"root"})}return n})(),u1=(()=>{class n extends Qv{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof js?this.updateStateMemento():t instanceof yr?this.commitTransition(i):t instanceof Gc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Yo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof ci&&!_M(t)?this.restoreHistory(i):t instanceof $s?this.restoreHistory(i,!0):t instanceof vr&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:s}=i,{replaceUrl:o,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||o){let c=this.browserPageId,l=oe(oe({},a),this.generateNgRouterState(s,c,i));this.location.replaceState(t,"",l)}else{let c=oe(oe({},a),this.generateNgRouterState(s,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,s=this.currentPageId-r;s!==0?this.location.historyGo(s):this.getCurrentUrlTree()===t.finalUrl&&s===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?oe({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):oe({navigationId:t},this.routerUrlState(r))}static \u0275fac=(()=>{let t;return function(r){return(t||(t=yc(n)))(r||n)}})();static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ey(n,e){n.events.pipe(or(t=>t instanceof vr||t instanceof ci||t instanceof $s||t instanceof yr),Ft(t=>t instanceof vr||t instanceof yr?0:(t instanceof ci?t.code===xn.Redirect||t.code===xn.SupersededByNewNavigation:!1)?2:1),or(t=>t!==2),ar(1)).subscribe(()=>{e()})}var Af=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=re(lv);stateManager=re(Qv);options=re(Qc,{optional:!0})||{};pendingTasks=re(Gr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=re(zM);urlSerializer=re(Zc);location=re(Wo);urlHandlingStrategy=re(If);injector=re(qt);_events=new an;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=re(WM);injectorCleanup=re(GM,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=re(el,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!re(Tf,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new on;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,s=Yi(this.navigationTransitions.currentNavigation);if(r!==null&&s!==null){if(this.stateManager.handleRouterEvent(i,s),i instanceof ci&&i.code!==xn.Redirect&&i.code!==xn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof vr)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Zo){let o=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=oe({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||c1(r.source)},o);this.scheduleNavigation(a,Bc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}lN(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Bc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,s)=>{this.navigateToSyncWithBrowser(t,r,i,s)})}navigateToSyncWithBrowser(t,i,r,s){let o=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(s=tt(oe({},s),{browserUrl:t})),r){let l=oe({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(s.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,o,s).catch(l=>{this.disposed||this.injector.get(qi)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Yi(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(Kv),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:s,fragment:o,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:o,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=oe(oe({},this.currentUrlTree.queryParams),s);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=s||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=mM(f)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return gM(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=Xo(t)?t:this.parseUrl(t),s=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(s,Bc,null,i)}navigate(t,i={skipLocationChange:!1}){return d1(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Ka(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=oe({},oM):i===!1?r=oe({},Ov):r=oe(oe({},Ov),i),Xo(t))return Yb(this.currentUrlTree,t,r);let s=this.parseUrl(t);return Yb(this.currentUrlTree,s,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,s])=>(s!=null&&(i[r]=s),i),{})}scheduleNavigation(t,i,r,s,o){if(this.disposed)return Promise.resolve(!1);let a,c,l;o?(a=o.resolve,c=o.reject,l=o.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return ey(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:s,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ue({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function d1(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Ae(4008,!1)}var p1=new Oe("");function ty(n,...e){return Mo([{provide:el,multi:!0,useValue:n},[],{provide:Zr,useFactory:m1},{provide:kd,multi:!0,useFactory:g1},e.map(t=>t.\u0275providers)])}function m1(){return re(Af).routerState.root}function g1(){let n=re(gi);return e=>{let t=n.get(Ho);if(e!==t.components[0])return;let i=n.get(Af),r=n.get(v1);n.get(y1)===1&&i.initialNavigation(),n.get(_1,null,{optional:!0})?.setUpPreloading(),n.get(p1,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var v1=new Oe("",{factory:()=>new an}),y1=new Oe("",{factory:()=>1});var _1=new Oe("");var cs={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},ls={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},vE=0,Ny=1,yE=2;var Ll=1,_E=2,Aa=3,us=0,Pn=1,Qn=2,nr=0,Da=1,Py=2,Ly=3,Oy=4,xE=5;var io=100,SE=101,bE=102,ME=103,EE=104,wE=200,TE=201,CE=202,IE=203,Fy=204,Uy=205,AE=206,DE=207,RE=208,NE=209,PE=210,LE=211,OE=212,FE=213,UE=214,sh=0,oh=1,ah=2,Sa=3,ch=4,lh=5,uh=6,dh=7,ky=0,kE=1,BE=2,Li=0,By=1,Vy=2,Hy=3,zy=4,Gy=5,Wy=6,jy=7;var wy=300,ds=301,ro=302,Uh=303,kh=304,Ol=306,fh=1e3,er=1001,hh=1002,sn=1003,VE=1004;var Fl=1005;var hn=1006,Bh=1007;var fs=1008;var ei=1009,$y=1010,qy=1011,Ra=1012,Vh=1013,Oi=1014,Fi=1015,Ui=1016,Hh=1017,zh=1018,Na=1020,Xy=35902,Yy=35899,Zy=1021,Ky=1022,di=1023,tr=1026,hs=1027,Jy=1028,Gh=1029,ps=1030,Wh=1031;var jh=1033,Ul=33776,kl=33777,Bl=33778,Vl=33779,$h=35840,qh=35841,Xh=35842,Yh=35843,Zh=36196,Kh=37492,Jh=37496,Qh=37488,ep=37489,Hl=37490,tp=37491,np=37808,ip=37809,rp=37810,sp=37811,op=37812,ap=37813,cp=37814,lp=37815,up=37816,dp=37817,fp=37818,hp=37819,pp=37820,mp=37821,gp=36492,vp=36494,yp=36495,_p=36283,xp=36284,zl=36285,Sp=36286;var ul=2300,ph=2301,ih=2302,Ty=2303,Cy=2400,Iy=2401,Ay=2402;var HE=3200;var Qy=0,zE=1,Ir="",Xn="srgb",dl="srgb-linear",fl="linear",_t="srgb";var rh=7680;var GE=519,WE=512,jE=513,$E=514,bp=515,qE=516,XE=517,Mp=518,YE=519,e_=35044;var t_="300 es",Ri=2e3,hl=2001;function S1(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function b1(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function pl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function ZE(){let n=pl("canvas");return n.style.display="block",n}var jM={},ba=null;function ml(...n){let e="THREE."+n.shift();ba?ba("log",e,...n):console.log(e,...n)}function KE(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Re(...n){n=KE(n);let e="THREE."+n.shift();if(ba)ba("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Le(...n){n=KE(n);let e="THREE."+n.shift();if(ba)ba("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Js(...n){let e=n.join(" ");e in jM||(jM[e]=!0,Re(...n))}function JE(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var QE={[sh]:oh,[ah]:uh,[ch]:dh,[Sa]:lh,[oh]:sh,[uh]:ah,[dh]:ch,[lh]:Sa},Pi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},Sn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],$M=1234567,_a=Math.PI/180,Ma=180/Math.PI;function Er(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Sn[n&255]+Sn[n>>8&255]+Sn[n>>16&255]+Sn[n>>24&255]+"-"+Sn[e&255]+Sn[e>>8&255]+"-"+Sn[e>>16&15|64]+Sn[e>>24&255]+"-"+Sn[t&63|128]+Sn[t>>8&255]+"-"+Sn[t>>16&255]+Sn[t>>24&255]+Sn[i&255]+Sn[i>>8&255]+Sn[i>>16&255]+Sn[i>>24&255]).toLowerCase()}function Ke(n,e,t){return Math.max(e,Math.min(t,n))}function n_(n,e){return(n%e+e)%e}function M1(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function E1(n,e,t){return n!==e?(t-n)/(e-n):0}function ll(n,e,t){return(1-t)*n+t*e}function w1(n,e,t,i){return ll(n,e,1-Math.exp(-t*i))}function T1(n,e=1){return e-Math.abs(n_(n,e*2)-e)}function C1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function I1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function A1(n,e){return n+Math.floor(Math.random()*(e-n+1))}function D1(n,e){return n+Math.random()*(e-n)}function R1(n){return n*(.5-Math.random())}function N1(n){n!==void 0&&($M=n);let e=$M+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function P1(n){return n*_a}function L1(n){return n*Ma}function O1(n){return n>0&&Number.isInteger(n)&&2**Math.round(Math.log2(n))===n}function F1(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function U1(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function k1(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),f=o((e-i)/2),h=s((i-e)/2),m=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*m,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*m,a*l);break;case"ZYZ":n.set(c*m,c*h,a*u,a*l);break;default:Re("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function Di(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:case Uint8ClampedArray:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("THREE.MathUtils: Invalid component type.")}}function St(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:case Uint8ClampedArray:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("THREE.MathUtils: Invalid component type.")}}var i_={DEG2RAD:_a,RAD2DEG:Ma,generateUUID:Er,clamp:Ke,euclideanModulo:n_,mapLinear:M1,inverseLerp:E1,lerp:ll,damp:w1,pingpong:T1,smoothstep:C1,smootherstep:I1,randInt:A1,randFloat:D1,randFloatSpread:R1,seededRandom:N1,degToRad:P1,radToDeg:L1,isPowerOfTwo:O1,ceilPowerOfTwo:F1,floorPowerOfTwo:U1,setQuaternionFromProperEuler:k1,normalize:St,denormalize:Di},Pe=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("THREE.Vector2: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("THREE.Vector2: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Zn=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],m=s[o+2],y=s[o+3];if(d!==y||c!==f||l!==h||u!==m){let g=c*f+l*h+u*m+d*y;g<0&&(f=-f,h=-h,m=-m,y=-y,g=-g);let p=1-a;if(g<.9995){let b=Math.acos(g),A=Math.sin(b);p=Math.sin(p*b)/A,a=Math.sin(a*b)/A,c=c*p+f*a,l=l*p+h*a,u=u*p+m*a,d=d*p+y*a}else{c=c*p+f*a,l=l*p+h*a,u=u*p+m*a,d=d*p+y*a;let b=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=b,l*=b,u*=b,d*=b}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],m=s[o+3];return e[t]=a*m+u*d+c*h-l*f,e[t+1]=c*m+u*f+l*d-a*h,e[t+2]=l*m+u*h+a*f-c*d,e[t+3]=u*m-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),m=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d-f*h*m;break;case"YXZ":this._x=f*u*d+l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d+f*h*m;break;case"ZXY":this._x=f*u*d-l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d-f*h*m;break;case"ZYX":this._x=f*u*d-l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d+f*h*m;break;case"YZX":this._x=f*u*d+l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d-f*h*m;break;case"XZY":this._x=f*u*d-l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d+f*h*m;break;default:Re("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Ke(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},F=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("THREE.Vector3: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("THREE.Vector3: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(qM.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(qM.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return ny.copy(this).projectOnVector(e),this.sub(ny)}reflect(e){return this.sub(ny.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},ny=new F,qM=new Zn,Be=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],m=i[8],y=r[0],g=r[3],p=r[6],b=r[1],A=r[4],S=r[7],M=r[2],E=r[5],I=r[8];return s[0]=o*y+a*b+c*M,s[3]=o*g+a*A+c*E,s[6]=o*p+a*S+c*I,s[1]=l*y+u*b+d*M,s[4]=l*g+u*A+d*E,s[7]=l*p+u*S+d*I,s[2]=f*y+h*b+m*M,s[5]=f*g+h*A+m*E,s[8]=f*p+h*S+m*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,m=t*d+i*f+r*h;if(m===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/m;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*o)*y,e[3]=f*y,e[4]=(u*t-r*c)*y,e[5]=(r*s-a*t)*y,e[6]=h*y,e[7]=(i*c-l*t)*y,e[8]=(o*t-i*s)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return Js("Matrix3: .scale() is deprecated. Use .makeScale() instead."),this.premultiply(iy.makeScale(e,t)),this}rotate(e){return Js("Matrix3: .rotate() is deprecated. Use .makeRotation() instead."),this.premultiply(iy.makeRotation(-e)),this}translate(e,t){return Js("Matrix3: .translate() is deprecated. Use .makeTranslation() instead."),this.premultiply(iy.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},iy=new Be,XM=new Be().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),YM=new Be().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function B1(){let n={enabled:!0,workingColorSpace:dl,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===_t&&(r.r=wr(r.r),r.g=wr(r.g),r.b=wr(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===_t&&(r.r=xa(r.r),r.g=xa(r.g),r.b=xa(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===Ir?fl:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return Js("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return Js("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[dl]:{primaries:e,whitePoint:i,transfer:fl,toXYZ:XM,fromXYZ:YM,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Xn},outputColorSpaceConfig:{drawingBufferColorSpace:Xn}},[Xn]:{primaries:e,whitePoint:i,transfer:_t,toXYZ:XM,fromXYZ:YM,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Xn}}}),n}var rt=B1();function wr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function xa(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var na,mh=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{na===void 0&&(na=pl("canvas")),na.width=e.width,na.height=e.height;let r=na.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=na}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=pl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=wr(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(wr(t[i]/255)*255):t[i]=wr(t[i]);return{data:t,width:e.width,height:e.height}}else return Re("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},V1=0,Ea=class{constructor(e=null){this.isTextureSource=!0,Object.defineProperty(this,"id",{value:V1++}),this.uuid=Er(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(ry(r[o].image)):s.push(ry(r[o]))}else s=ry(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function ry(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?mh.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Re("Texture: Unable to serialize Texture."),{})}var H1=0,sy=new F,ir=(()=>{class n extends Pi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=er,s=er,o=hn,a=fs,c=di,l=ei,u=n.DEFAULT_ANISOTROPY,d=Ir){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:H1++}),this.uuid=Er(),this.name="",this.source=new Ea(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new Pe(0,0),this.repeat=new Pe(1,1),this.center=new Pe(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Be,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(sy).x}get height(){return this.source.getSize(sy).y}get depth(){return this.source.getSize(sy).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Re(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Re(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==wy)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case fh:t.x=t.x-Math.floor(t.x);break;case er:t.x=t.x<0?0:1;break;case hh:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case fh:t.y=t.y-Math.floor(t.y);break;case er:t.y=t.y<0?0:1;break;case hh:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=wy,n.DEFAULT_ANISOTROPY=1,n})(),Ot=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("THREE.Vector4: index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("THREE.Vector4: index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],m=c[9],y=c[2],g=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-y)<.01&&Math.abs(m-g)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+y)<.1&&Math.abs(m+g)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let A=(l+1)/2,S=(h+1)/2,M=(p+1)/2,E=(u+f)/4,I=(d+y)/4,_=(m+g)/4;return A>S&&A>M?A<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(A),r=E/i,s=I/i):S>M?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=E/r,s=_/r):M<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(M),i=I/s,r=_/s),this.set(i,r,s,t),this}let b=Math.sqrt((g-m)*(g-m)+(d-y)*(d-y)+(f-u)*(f-u));return Math.abs(b)<.001&&(b=1),this.x=(g-m)/b,this.y=(d-y)/b,this.z=(f-u)/b,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this.w=Ke(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this.w=Ke(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},gh=class extends Pi{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:hn,depthBuffer:!0,stencilBuffer:!1,resolveColorBuffer:!0,resolveDepthBuffer:!0,resolveStencilBuffer:!0,storeMultisampledColorBuffer:!0,storeMultisampledDepthBuffer:!0,storeMultisampledStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1,useArrayDepthTexture:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Ot(0,0,e,t),this.scissorTest=!1,this.viewport=new Ot(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new ir(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveColorBuffer=i.resolveColorBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.storeMultisampledColorBuffer=i.storeMultisampledColorBuffer,this.storeMultisampledDepthBuffer=i.storeMultisampledDepthBuffer,this.storeMultisampledStencilBuffer=i.storeMultisampledStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview,this.useArrayDepthTexture=i.useArrayDepthTexture}_setTextureOptions(e={}){let t={minFilter:hn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&this._depthTexture.renderTarget===this&&(this._depthTexture.renderTarget=null),e!==null&&e.renderTarget===null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new Ea(r)}if(this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveColorBuffer=e.resolveColorBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,this.storeMultisampledColorBuffer=e.storeMultisampledColorBuffer,this.storeMultisampledDepthBuffer=e.storeMultisampledDepthBuffer,this.storeMultisampledStencilBuffer=e.storeMultisampledStencilBuffer,e.depthTexture!==null)if(e.depthTexture.renderTarget===e){let t=e.depthTexture.clone();t.renderTarget=null,this.depthTexture=t}else this.depthTexture=e.depthTexture;return this.samples=e.samples,this.multiview=e.multiview,this.useArrayDepthTexture=e.useArrayDepthTexture,this}dispose(){this.dispatchEvent({type:"dispose"})}},Un=class extends gh{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},gl=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=er,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}copy(e){return super.copy(e),this.wrapR=e.wrapR,this}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var vh=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=er,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}copy(e){return super.copy(e),this.wrapR=e.wrapR,this}};var Lt=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,m,y,g){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,m,y,g)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,m,y,g){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=m,p[11]=y,p[15]=g,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinantAffine()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinantAffine()===0)return this.identity();let t=this.elements,i=e.elements,r=1/ia.setFromMatrixColumn(e,0).length(),s=1/ia.setFromMatrixColumn(e,1).length(),o=1/ia.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,m=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+m*l,t[5]=f-y*l,t[9]=-a*c,t[2]=y-f*l,t[6]=m+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,m=l*u,y=l*d;t[0]=f+y*a,t[4]=m*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-m,t[6]=y+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,m=l*u,y=l*d;t[0]=f-y*a,t[4]=-o*d,t[8]=m+h*a,t[1]=h+m*a,t[5]=o*u,t[9]=y-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,m=a*u,y=a*d;t[0]=c*u,t[4]=m*l-h,t[8]=f*l+y,t[1]=c*d,t[5]=y*l+f,t[9]=h*l-m,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,m=a*c,y=a*l;t[0]=c*u,t[4]=y-f*d,t[8]=m*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+m,t[10]=f-y*d}else if(e.order==="XZY"){let f=o*c,h=o*l,m=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+y,t[5]=o*u,t[9]=h*d-m,t[2]=m*d-h,t[6]=a*u,t[10]=y*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(z1,e,G1)}lookAt(e,t,i){let r=this.elements;return $n.subVectors(e,t),$n.lengthSq()===0&&($n.z=1),$n.normalize(),Kr.crossVectors(i,$n),Kr.lengthSq()===0&&(Math.abs(i.z)===1?$n.x+=1e-4:$n.z+=1e-4,$n.normalize(),Kr.crossVectors(i,$n)),Kr.normalize(),Df.crossVectors($n,Kr),r[0]=Kr.x,r[4]=Df.x,r[8]=$n.x,r[1]=Kr.y,r[5]=Df.y,r[9]=$n.y,r[2]=Kr.z,r[6]=Df.z,r[10]=$n.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],m=i[2],y=i[6],g=i[10],p=i[14],b=i[3],A=i[7],S=i[11],M=i[15],E=r[0],I=r[4],_=r[8],T=r[12],R=r[1],O=r[5],C=r[9],V=r[13],D=r[2],U=r[6],$=r[10],B=r[14],X=r[3],W=r[7],Q=r[11],ne=r[15];return s[0]=o*E+a*R+c*D+l*X,s[4]=o*I+a*O+c*U+l*W,s[8]=o*_+a*C+c*$+l*Q,s[12]=o*T+a*V+c*B+l*ne,s[1]=u*E+d*R+f*D+h*X,s[5]=u*I+d*O+f*U+h*W,s[9]=u*_+d*C+f*$+h*Q,s[13]=u*T+d*V+f*B+h*ne,s[2]=m*E+y*R+g*D+p*X,s[6]=m*I+y*O+g*U+p*W,s[10]=m*_+y*C+g*$+p*Q,s[14]=m*T+y*V+g*B+p*ne,s[3]=b*E+A*R+S*D+M*X,s[7]=b*I+A*O+S*U+M*W,s[11]=b*_+A*C+S*$+M*Q,s[15]=b*T+A*V+S*B+M*ne,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],m=e[3],y=e[7],g=e[11],p=e[15],b=c*h-l*f,A=a*h-l*d,S=a*f-c*d,M=o*h-l*u,E=o*f-c*u,I=o*d-a*u;return t*(y*b-g*A+p*S)-i*(m*b-g*M+p*E)+r*(m*A-y*M+p*I)-s*(m*S-y*E+g*I)}determinantAffine(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[1],o=e[5],a=e[9],c=e[2],l=e[6],u=e[10];return t*(o*u-a*l)-i*(s*u-a*c)+r*(s*l-o*c)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],m=e[12],y=e[13],g=e[14],p=e[15],b=t*a-i*o,A=t*c-r*o,S=t*l-s*o,M=i*c-r*a,E=i*l-s*a,I=r*l-s*c,_=u*y-d*m,T=u*g-f*m,R=u*p-h*m,O=d*g-f*y,C=d*p-h*y,V=f*p-h*g,D=b*V-A*C+S*O+M*R-E*T+I*_;if(D===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let U=1/D;return e[0]=(a*V-c*C+l*O)*U,e[1]=(r*C-i*V-s*O)*U,e[2]=(y*I-g*E+p*M)*U,e[3]=(f*E-d*I-h*M)*U,e[4]=(c*R-o*V-l*T)*U,e[5]=(t*V-r*R+s*T)*U,e[6]=(g*S-m*I-p*A)*U,e[7]=(u*I-f*S+h*A)*U,e[8]=(o*C-a*R+l*_)*U,e[9]=(i*R-t*C-s*_)*U,e[10]=(m*E-y*S+p*b)*U,e[11]=(d*S-u*E-h*b)*U,e[12]=(a*T-o*O-c*_)*U,e[13]=(t*O-i*T+r*_)*U,e[14]=(y*A-m*M-g*b)*U,e[15]=(u*M-d*A+f*b)*U,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,m=s*d,y=o*u,g=o*d,p=a*d,b=c*l,A=c*u,S=c*d,M=i.x,E=i.y,I=i.z;return r[0]=(1-(y+p))*M,r[1]=(h+S)*M,r[2]=(m-A)*M,r[3]=0,r[4]=(h-S)*E,r[5]=(1-(f+p))*E,r[6]=(g+b)*E,r[7]=0,r[8]=(m+A)*I,r[9]=(g-b)*I,r[10]=(1-(f+y))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinantAffine();if(s===0)return i.set(1,1,1),t.identity(),this;let o=ia.set(r[0],r[1],r[2]).length(),a=ia.set(r[4],r[5],r[6]).length(),c=ia.set(r[8],r[9],r[10]).length();s<0&&(o=-o),Ci.copy(this);let l=1/o,u=1/a,d=1/c;return Ci.elements[0]*=l,Ci.elements[1]*=l,Ci.elements[2]*=l,Ci.elements[4]*=u,Ci.elements[5]*=u,Ci.elements[6]*=u,Ci.elements[8]*=d,Ci.elements[9]*=d,Ci.elements[10]*=d,t.setFromRotationMatrix(Ci),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=Ri,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),m,y;if(c)m=s/(o-s),y=o*s/(o-s);else if(a===Ri)m=-(o+s)/(o-s),y=-2*o*s/(o-s);else if(a===hl)m=-o/(o-s),y=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=m,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Ri,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),m,y;if(c)m=1/(o-s),y=o/(o-s);else if(a===Ri)m=-2/(o-s),y=-(o+s)/(o-s);else if(a===hl)m=-1/(o-s),y=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=m,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},ia=new F,Ci=new Lt,z1=new F(0,0,0),G1=new F(1,1,1),Kr=new F,Df=new F,$n=new F,ZM=new Lt,KM=new Zn,vl=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],f=s[2],h=s[6],m=s[10];switch(i){case"XYZ":this._y=Math.asin(Ke(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,m),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Ke(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,m),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(Ke(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,m),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Ke(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,m),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Ke(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(c,m));break;case"XZY":this._z=Math.asin(-Ke(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,m),this._y=0);break;default:Re("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return ZM.makeRotationFromQuaternion(t),this.setFromRotationMatrix(ZM,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return KM.setFromEuler(this),this.setFromQuaternion(KM,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),yl=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},W1=0,JM=new F,ra=new Zn,_r=new Lt,Rf=new F,tl=new F,j1=new F,$1=new Zn,QM=new F(1,0,0),eE=new F(0,1,0),tE=new F(0,0,1),nE={type:"added"},q1={type:"removed"},sa={type:"childadded",child:null},oy={type:"childremoved",child:null},Ar=(()=>{class n extends Pi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:W1++}),this.uuid=Er(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new F,i=new vl,r=new Zn,s=new F(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Lt},normalMatrix:{value:new Be}}),this.matrix=new Lt,this.matrixWorld=new Lt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new yl,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return ra.setFromAxisAngle(t,i),this.quaternion.multiply(ra),this}rotateOnWorldAxis(t,i){return ra.setFromAxisAngle(t,i),this.quaternion.premultiply(ra),this}rotateX(t){return this.rotateOnAxis(QM,t)}rotateY(t){return this.rotateOnAxis(eE,t)}rotateZ(t){return this.rotateOnAxis(tE,t)}translateOnAxis(t,i){return JM.copy(t).applyQuaternion(this.quaternion),this.position.add(JM.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(QM,t)}translateY(t){return this.translateOnAxis(eE,t)}translateZ(t){return this.translateOnAxis(tE,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(_r.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Rf.copy(t):Rf.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),tl.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?_r.lookAt(tl,Rf,this.up):_r.lookAt(Rf,tl,this.up),this.quaternion.setFromRotationMatrix(_r),s&&(_r.extractRotation(s.matrixWorld),ra.setFromRotationMatrix(_r),this.quaternion.premultiply(ra.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Le("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(nE),sa.child=t,this.dispatchEvent(sa),sa.child=null):Le("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(q1),oy.child=t,this.dispatchEvent(oy),oy.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),_r.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),_r.multiply(t.parent.matrixWorld)),t.applyMatrix4(_r),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(nE),sa.child=t,this.dispatchEvent(sa),sa.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(tl,t,j1),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(tl,$1,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}intersectsFrustum(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i,r=!1){let s=this.parent;if(t===!0&&s!==null&&s.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||r)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,r=!0),i===!0){let o=this.children;for(let a=0,c=o.length;a<c;a++)o[a].updateWorldMatrix(!1,!0,r)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,s.name=this.name,s.castShadow=this.castShadow,s.receiveShadow=this.receiveShadow,s.visible=this.visible,s.frustumCulled=this.frustumCulled,s.renderOrder=this.renderOrder,s.static=this.static,s.matrixAutoUpdate=this.matrixAutoUpdate,Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>tt(oe({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>oe({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];o(t.shapes,f)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),m=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),m.length>0&&(r.animations=m),y.length>0&&(r.nodes=y)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}dispose(){this.dispatchEvent({type:"dispose"})}}return n.DEFAULT_UP=new F(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Ni=class extends Ar{constructor(){super(),this.isGroup=!0,this.type="Group"}},X1={type:"move"},wa=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Ni,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Ni,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new F,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new F),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Ni,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new F,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new F,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let y of e.hand.values()){let g=t.getJointPose(y,i),p=this._getHandJoint(l,y);g!==null&&(p.matrix.fromArray(g.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=g.radius),p.visible=g!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,m=.005;l.inputState.pinching&&f>h+m?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-m&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(X1)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Ni;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},ew={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Jr={h:0,s:0,l:0},Nf={h:0,s:0,l:0};function ay(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var st=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Xn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,rt.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=rt.workingColorSpace){return this.r=e,this.g=t,this.b=i,rt.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=rt.workingColorSpace){if(e=n_(e,1),t=Ke(t,0,1),i=Ke(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=ay(o,s,e+1/3),this.g=ay(o,s,e),this.b=ay(o,s,e-1/3)}return rt.colorSpaceToWorking(this,r),this}setStyle(e,t=Xn){function i(s){s!==void 0&&parseFloat(s)<1&&Re("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Re("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Re("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Xn){let i=ew[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Re("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=wr(e.r),this.g=wr(e.g),this.b=wr(e.b),this}copyLinearToSRGB(e){return this.r=xa(e.r),this.g=xa(e.g),this.b=xa(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Xn){return rt.workingToColorSpace(bn.copy(this),e),Math.round(Ke(bn.r*255,0,255))*65536+Math.round(Ke(bn.g*255,0,255))*256+Math.round(Ke(bn.b*255,0,255))}getHexString(e=Xn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=rt.workingColorSpace){rt.workingToColorSpace(bn.copy(this),t);let i=bn.r,r=bn.g,s=bn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=rt.workingColorSpace){return rt.workingToColorSpace(bn.copy(this),t),e.r=bn.r,e.g=bn.g,e.b=bn.b,e}getStyle(e=Xn){rt.workingToColorSpace(bn.copy(this),e);let t=bn.r,i=bn.g,r=bn.b;return e!==Xn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Jr),this.setHSL(Jr.h+e,Jr.s+t,Jr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Jr),e.getHSL(Nf);let i=ll(Jr.h,Nf.h,t),r=ll(Jr.s,Nf.s,t),s=ll(Jr.l,Nf.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},bn=new st;st.NAMES=ew;var _l=class extends Ar{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new vl,this.environmentIntensity=1,this.environmentRotation=new vl,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),t.object.backgroundBlurriness=this.backgroundBlurriness,t.object.backgroundIntensity=this.backgroundIntensity,t.object.backgroundRotation=this.backgroundRotation.toArray(),t.object.environmentIntensity=this.environmentIntensity,t.object.environmentRotation=this.environmentRotation.toArray(),t}},Ii=new F,xr=new F,cy=new F,Sr=new F,oa=new F,aa=new F,iE=new F,ly=new F,uy=new F,dy=new F,fy=new Ot,hy=new Ot,py=new Ot,Qi=class n{constructor(e=new F,t=new F,i=new F){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Ii.subVectors(e,t),r.cross(Ii);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Ii.subVectors(r,t),xr.subVectors(i,t),cy.subVectors(e,t);let o=Ii.dot(Ii),a=Ii.dot(xr),c=Ii.dot(cy),l=xr.dot(xr),u=xr.dot(cy),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,m=(o*u-a*c)*f;return s.set(1-h-m,m,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Sr)===null?!1:Sr.x>=0&&Sr.y>=0&&Sr.x+Sr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Sr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Sr.x),c.addScaledVector(o,Sr.y),c.addScaledVector(a,Sr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return fy.setScalar(0),hy.setScalar(0),py.setScalar(0),fy.fromBufferAttribute(e,t),hy.fromBufferAttribute(e,i),py.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(fy,s.x),o.addScaledVector(hy,s.y),o.addScaledVector(py,s.z),o}static isFrontFacing(e,t,i,r){return Ii.subVectors(i,t),xr.subVectors(e,t),Ii.cross(xr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Ii.subVectors(this.c,this.b),xr.subVectors(this.a,this.b),Ii.cross(xr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;oa.subVectors(r,i),aa.subVectors(s,i),ly.subVectors(e,i);let c=oa.dot(ly),l=aa.dot(ly);if(c<=0&&l<=0)return t.copy(i);uy.subVectors(e,r);let u=oa.dot(uy),d=aa.dot(uy);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(oa,o);dy.subVectors(e,s);let h=oa.dot(dy),m=aa.dot(dy);if(m>=0&&h<=m)return t.copy(s);let y=h*l-c*m;if(y<=0&&l>=0&&m<=0)return a=l/(l-m),t.copy(i).addScaledVector(aa,a);let g=u*m-h*d;if(g<=0&&d-u>=0&&h-m>=0)return iE.subVectors(s,r),a=(d-u)/(d-u+(h-m)),t.copy(r).addScaledVector(iE,a);let p=1/(g+y+f);return o=y*p,a=f*p,t.copy(i).addScaledVector(oa,o).addScaledVector(aa,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},ns=class{constructor(e=new F(1/0,1/0,1/0),t=new F(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Ai.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Ai.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Ai.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Ai):Ai.fromBufferAttribute(s,o),Ai.applyMatrix4(e.matrixWorld),this.expandByPoint(Ai);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Pf.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Pf.copy(i.boundingBox)),Pf.applyMatrix4(e.matrixWorld),this.union(Pf)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Ai),Ai.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(nl),Lf.subVectors(this.max,nl),ca.subVectors(e.a,nl),la.subVectors(e.b,nl),ua.subVectors(e.c,nl),Qr.subVectors(la,ca),es.subVectors(ua,la),Xs.subVectors(ca,ua);let t=[0,-Qr.z,Qr.y,0,-es.z,es.y,0,-Xs.z,Xs.y,Qr.z,0,-Qr.x,es.z,0,-es.x,Xs.z,0,-Xs.x,-Qr.y,Qr.x,0,-es.y,es.x,0,-Xs.y,Xs.x,0];return!my(t,ca,la,ua,Lf)||(t=[1,0,0,0,1,0,0,0,1],!my(t,ca,la,ua,Lf))?!1:(Of.crossVectors(Qr,es),t=[Of.x,Of.y,Of.z],my(t,ca,la,ua,Lf))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Ai).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Ai).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(br[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),br[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),br[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),br[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),br[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),br[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),br[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),br[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(br),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},br=[new F,new F,new F,new F,new F,new F,new F,new F],Ai=new F,Pf=new ns,ca=new F,la=new F,ua=new F,Qr=new F,es=new F,Xs=new F,nl=new F,Lf=new F,Of=new F,Ys=new F;function my(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Ys.fromArray(n,s);let a=r.x*Math.abs(Ys.x)+r.y*Math.abs(Ys.y)+r.z*Math.abs(Ys.z),c=e.dot(Ys),l=t.dot(Ys),u=i.dot(Ys);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Wt=new F,Ff=new Pe,Y1=0,fn=class extends Pi{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:Y1++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=e_,this.updateRanges=[],this.gpuType=Fi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Ff.fromBufferAttribute(this,t),Ff.applyMatrix3(e),this.setXY(t,Ff.x,Ff.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyMatrix3(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyMatrix4(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyNormalMatrix(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.transformDirection(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Di(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=St(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Di(t,this.array)),t}setX(e,t){return this.normalized&&(t=St(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Di(t,this.array)),t}setY(e,t){return this.normalized&&(t=St(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Di(t,this.array)),t}setZ(e,t){return this.normalized&&(t=St(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Di(t,this.array)),t}setW(e,t){return this.normalized&&(t=St(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=St(t,this.array),i=St(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=St(t,this.array),i=St(i,this.array),r=St(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=St(t,this.array),i=St(i,this.array),r=St(r,this.array),s=St(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return e.name=this.name,e.usage=this.usage,e.gpuType=this.gpuType,e}dispose(){this.dispatchEvent({type:"dispose"})}};var xl=class extends fn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Sl=class extends fn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var jt=class extends fn{constructor(e,t,i){super(new Float32Array(e),t,i)}},Z1=new ns,il=new F,gy=new F,Qs=class{constructor(e=new F,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):Z1.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;il.subVectors(e,this.center);let t=il.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(il,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(gy.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(il.copy(e.center).add(gy)),this.expandByPoint(il.copy(e.center).sub(gy))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},K1=0,ui=new Lt,vy=new Ar,da=new F,qn=new ns,rl=new ns,rn=new F,pn=class n extends Pi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:K1++}),this.uuid=Er(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={},this._transformed=!1}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(S1(e)?Sl:xl)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Be().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this._transformed=!0,this}applyQuaternion(e){return ui.makeRotationFromQuaternion(e),this.applyMatrix4(ui),this}rotateX(e){return ui.makeRotationX(e),this.applyMatrix4(ui),this}rotateY(e){return ui.makeRotationY(e),this.applyMatrix4(ui),this}rotateZ(e){return ui.makeRotationZ(e),this.applyMatrix4(ui),this}translate(e,t,i){return ui.makeTranslation(e,t,i),this.applyMatrix4(ui),this}scale(e,t,i){return ui.makeScale(e,t,i),this.applyMatrix4(ui),this}lookAt(e){return vy.lookAt(e),vy.updateMatrix(),this.applyMatrix4(vy.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(da).negate(),this.translate(da.x,da.y,da.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new jt(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Re("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new ns);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Le("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new F(-1/0,-1/0,-1/0),new F(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];qn.setFromBufferAttribute(s),this.morphTargetsRelative?(rn.addVectors(this.boundingBox.min,qn.min),this.boundingBox.expandByPoint(rn),rn.addVectors(this.boundingBox.max,qn.max),this.boundingBox.expandByPoint(rn)):(this.boundingBox.expandByPoint(qn.min),this.boundingBox.expandByPoint(qn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Le('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Qs);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Le("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new F,1/0);return}if(e){let i=this.boundingSphere.center;if(qn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];rl.setFromBufferAttribute(a),this.morphTargetsRelative?(rn.addVectors(qn.min,rl.min),qn.expandByPoint(rn),rn.addVectors(qn.max,rl.max),qn.expandByPoint(rn)):(qn.expandByPoint(rl.min),qn.expandByPoint(rl.max))}qn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)rn.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(rn));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)rn.fromBufferAttribute(a,l),c&&(da.fromBufferAttribute(e,l),rn.add(da)),r=Math.max(r,i.distanceToSquared(rn))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Le('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Le("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv,o=this.getAttribute("tangent");(o===void 0||o.count!==i.count)&&(o=new fn(new Float32Array(4*i.count),4),this.setAttribute("tangent",o));let a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new F,c[_]=new F;let l=new F,u=new F,d=new F,f=new Pe,h=new Pe,m=new Pe,y=new F,g=new F;function p(_,T,R){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,T),d.fromBufferAttribute(i,R),f.fromBufferAttribute(s,_),h.fromBufferAttribute(s,T),m.fromBufferAttribute(s,R),u.sub(l),d.sub(l),h.sub(f),m.sub(f);let O=1/(h.x*m.y-m.x*h.y);isFinite(O)&&(y.copy(u).multiplyScalar(m.y).addScaledVector(d,-h.y).multiplyScalar(O),g.copy(d).multiplyScalar(h.x).addScaledVector(u,-m.x).multiplyScalar(O),a[_].add(y),a[T].add(y),a[R].add(y),c[_].add(g),c[T].add(g),c[R].add(g))}let b=this.groups;b.length===0&&(b=[{start:0,count:e.count}]);for(let _=0,T=b.length;_<T;++_){let R=b[_],O=R.start,C=R.count;for(let V=O,D=O+C;V<D;V+=3)p(e.getX(V+0),e.getX(V+1),e.getX(V+2))}let A=new F,S=new F,M=new F,E=new F;function I(_){M.fromBufferAttribute(r,_),E.copy(M);let T=a[_];A.copy(T),A.sub(M.multiplyScalar(M.dot(T))).normalize(),S.crossVectors(E,T);let O=S.dot(c[_])<0?-1:1;o.setXYZW(_,A.x,A.y,A.z,O)}for(let _=0,T=b.length;_<T;++_){let R=b[_],O=R.start,C=R.count;for(let V=O,D=O+C;V<D;V+=3)I(e.getX(V+0)),I(e.getX(V+1)),I(e.getX(V+2))}this._transformed=!0}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0||i.count!==t.count)i=new fn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new F,s=new F,o=new F,a=new F,c=new F,l=new F,u=new F,d=new F;if(e)for(let f=0,h=e.count;f<h;f+=3){let m=e.getX(f+0),y=e.getX(f+1),g=e.getX(f+2);r.fromBufferAttribute(t,m),s.fromBufferAttribute(t,y),o.fromBufferAttribute(t,g),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,m),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,g),a.add(u),c.add(u),l.add(u),i.setXYZ(m,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(g,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)rn.fromBufferAttribute(e,t),rn.normalize(),e.setXYZ(t,rn.x,rn.y,rn.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,m=0;for(let y=0,g=c.length;y<g;y++){a.isInterleavedBufferAttribute?h=c[y]*a.data.stride+a.offset:h=c[y]*u;for(let p=0;p<u;p++)f[m++]=l[h++]}return new fn(f,u,d)}if(this.index===null)return Re("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.parameters!==void 0&&this._transformed===!0?"BufferGeometry":this.type,e.name=this.name,Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0&&this._transformed!==!0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this._transformed=e._transformed,this}dispose(){this.dispatchEvent({type:"dispose"})}},yh=class{constructor(e,t){this.isInterleavedBuffer=!0,this.array=e,this.stride=t,this.count=e!==void 0?e.length/t:0,this.usage=e_,this.updateRanges=[],this.version=0,this.uuid=Er()}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.array=new e.array.constructor(e.array),this.count=e.count,this.stride=e.stride,this.usage=e.usage,this}copyAt(e,t,i){e*=this.stride,i*=t.stride;for(let r=0,s=this.stride;r<s;r++)this.array[e+r]=t.array[i+r];return this}set(e,t=0){return this.array.set(e,t),this}clone(e){e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=Er()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=this.array.slice(0).buffer);let t=new this.array.constructor(e.arrayBuffers[this.array.buffer._uuid]),i=new this.constructor(t,this.stride);return i.setUsage(this.usage),i}onUpload(e){return this.onUploadCallback=e,this}toJSON(e){e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=Er()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=Array.from(new Uint32Array(this.array.buffer)));let t={uuid:this.uuid,buffer:this.array.buffer._uuid,type:this.array.constructor.name,stride:this.stride};return t.usage=this.usage,t}},Nn=new F,bl=class n{constructor(e,t,i,r=!1){this.isInterleavedBufferAttribute=!0,this.name="",this.data=e,this.itemSize=t,this.offset=i,this.normalized=r}get count(){return this.data.count}get array(){return this.data.array}set needsUpdate(e){this.data.needsUpdate=e}applyMatrix4(e){for(let t=0,i=this.data.count;t<i;t++)Nn.fromBufferAttribute(this,t),Nn.applyMatrix4(e),this.setXYZ(t,Nn.x,Nn.y,Nn.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Nn.fromBufferAttribute(this,t),Nn.applyNormalMatrix(e),this.setXYZ(t,Nn.x,Nn.y,Nn.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Nn.fromBufferAttribute(this,t),Nn.transformDirection(e),this.setXYZ(t,Nn.x,Nn.y,Nn.z);return this}getComponent(e,t){let i=this.array[e*this.data.stride+this.offset+t];return this.normalized&&(i=Di(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=St(i,this.array)),this.data.array[e*this.data.stride+this.offset+t]=i,this}setX(e,t){return this.normalized&&(t=St(t,this.array)),this.data.array[e*this.data.stride+this.offset]=t,this}setY(e,t){return this.normalized&&(t=St(t,this.array)),this.data.array[e*this.data.stride+this.offset+1]=t,this}setZ(e,t){return this.normalized&&(t=St(t,this.array)),this.data.array[e*this.data.stride+this.offset+2]=t,this}setW(e,t){return this.normalized&&(t=St(t,this.array)),this.data.array[e*this.data.stride+this.offset+3]=t,this}getX(e){let t=this.data.array[e*this.data.stride+this.offset];return this.normalized&&(t=Di(t,this.array)),t}getY(e){let t=this.data.array[e*this.data.stride+this.offset+1];return this.normalized&&(t=Di(t,this.array)),t}getZ(e){let t=this.data.array[e*this.data.stride+this.offset+2];return this.normalized&&(t=Di(t,this.array)),t}getW(e){let t=this.data.array[e*this.data.stride+this.offset+3];return this.normalized&&(t=Di(t,this.array)),t}setXY(e,t,i){return e=e*this.data.stride+this.offset,this.normalized&&(t=St(t,this.array),i=St(i,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this}setXYZ(e,t,i,r){return e=e*this.data.stride+this.offset,this.normalized&&(t=St(t,this.array),i=St(i,this.array),r=St(r,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e=e*this.data.stride+this.offset,this.normalized&&(t=St(t,this.array),i=St(i,this.array),r=St(r,this.array),s=St(s,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this.data.array[e+3]=s,this}clone(e){if(e===void 0){ml("InterleavedBufferAttribute.clone(): Cloning an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return new fn(new this.array.constructor(t),this.itemSize,this.normalized)}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.clone(e)),new n(e.interleavedBuffers[this.data.uuid],this.itemSize,this.offset,this.normalized)}toJSON(e){if(e===void 0){ml("InterleavedBufferAttribute.toJSON(): Serializing an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return{itemSize:this.itemSize,type:this.array.constructor.name,array:t,normalized:this.normalized}}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.toJSON(e)),{isInterleavedBufferAttribute:!0,itemSize:this.itemSize,data:this.data.uuid,offset:this.offset,normalized:this.normalized}}},yy=new F,J1=new F,Q1=new Be,Yn=class{constructor(e=new F(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=yy.subVectors(i,t).cross(J1.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(yy),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||Q1.getNormalMatrix(e),r=this.coplanarPoint(yy).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}toJSON(){return{normal:this.normal.toArray(),constant:this.constant}}fromJSON(e){return this.normal.fromArray(e.normal),this.constant=e.constant,this}},eP=0,Tr=class extends Pi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:eP++}),this.uuid=Er(),this.name="",this.type="Material",this.blending=Da,this.side=us,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Fy,this.blendDst=Uy,this.blendEquation=io,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new st(0,0,0),this.blendAlpha=0,this.depthFunc=Sa,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=GE,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=rh,this.stencilZFail=rh,this.stencilZPass=rh,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Re(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Re(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector2&&i&&i.isVector2||r&&r.isEuler&&i&&i.isEuler||r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,i.blending=this.blending,i.side=this.side,i.shadowSide=this.shadowSide,i.vertexColors=this.vertexColors,i.opacity=this.opacity,i.transparent=this.transparent,i.blendSrc=this.blendSrc,i.blendDst=this.blendDst,i.blendEquation=this.blendEquation,i.blendSrcAlpha=this.blendSrcAlpha,i.blendDstAlpha=this.blendDstAlpha,i.blendEquationAlpha=this.blendEquationAlpha,i.blendColor=this.blendColor.getHex(),i.blendAlpha=this.blendAlpha,i.depthFunc=this.depthFunc,i.depthTest=this.depthTest,i.depthWrite=this.depthWrite,i.colorWrite=this.colorWrite,i.clipIntersection=this.clipIntersection,i.clipShadows=this.clipShadows,i.stencilWriteMask=this.stencilWriteMask,i.stencilFunc=this.stencilFunc,i.stencilRef=this.stencilRef,i.stencilFuncMask=this.stencilFuncMask,i.stencilFail=this.stencilFail,i.stencilZFail=this.stencilZFail,i.stencilZPass=this.stencilZPass,i.stencilWrite=this.stencilWrite,i.polygonOffset=this.polygonOffset,i.polygonOffsetFactor=this.polygonOffsetFactor,i.polygonOffsetUnits=this.polygonOffsetUnits,i.dithering=this.dithering,i.alphaTest=this.alphaTest,i.alphaHash=this.alphaHash,i.alphaToCoverage=this.alphaToCoverage,i.premultipliedAlpha=this.premultipliedAlpha,i.forceSinglePass=this.forceSinglePass,i.allowOverride=this.allowOverride,i.visible=this.visible,i.toneMapped=this.toneMapped,i.name=this.name,this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.retroreflectivity!==void 0&&(i.retroreflectivity=this.retroreflectivity),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),Array.isArray(this.clippingPlanes)&&this.clippingPlanes.length>0&&(i.clippingPlanes=this.clippingPlanes.map(s=>s.toJSON())),this.rotation!==void 0&&(i.rotation=this.rotation),this.depthPacking!==void 0&&(i.depthPacking=this.depthPacking),this.linewidth!==void 0&&(i.linewidth=this.linewidth),this.linecap!==void 0&&(i.linecap=this.linecap),this.linejoin!==void 0&&(i.linejoin=this.linejoin),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.wireframe!==void 0&&(i.wireframe=this.wireframe),this.wireframeLinewidth!==void 0&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!==void 0&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!==void 0&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading!==void 0&&(i.flatShading=this.flatShading),this.fog!==void 0&&(i.fog=this.fog),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}fromJSON(e,t){if(e.uuid!==void 0&&(this.uuid=e.uuid),e.name!==void 0&&(this.name=e.name),e.color!==void 0&&this.color!==void 0&&this.color.setHex(e.color),e.roughness!==void 0&&(this.roughness=e.roughness),e.metalness!==void 0&&(this.metalness=e.metalness),e.sheen!==void 0&&(this.sheen=e.sheen),e.sheenColor!==void 0&&(this.sheenColor=new st().setHex(e.sheenColor)),e.sheenRoughness!==void 0&&(this.sheenRoughness=e.sheenRoughness),e.emissive!==void 0&&this.emissive!==void 0&&this.emissive.setHex(e.emissive),e.specular!==void 0&&this.specular!==void 0&&this.specular.setHex(e.specular),e.specularIntensity!==void 0&&(this.specularIntensity=e.specularIntensity),e.specularColor!==void 0&&this.specularColor!==void 0&&this.specularColor.setHex(e.specularColor),e.shininess!==void 0&&(this.shininess=e.shininess),e.clearcoat!==void 0&&(this.clearcoat=e.clearcoat),e.clearcoatRoughness!==void 0&&(this.clearcoatRoughness=e.clearcoatRoughness),e.dispersion!==void 0&&(this.dispersion=e.dispersion),e.retroreflectivity!==void 0&&(this.retroreflectivity=e.retroreflectivity),e.iridescence!==void 0&&(this.iridescence=e.iridescence),e.iridescenceIOR!==void 0&&(this.iridescenceIOR=e.iridescenceIOR),e.iridescenceThicknessRange!==void 0&&(this.iridescenceThicknessRange=e.iridescenceThicknessRange),e.transmission!==void 0&&(this.transmission=e.transmission),e.thickness!==void 0&&(this.thickness=e.thickness),e.attenuationDistance!==void 0&&(this.attenuationDistance=e.attenuationDistance),e.attenuationColor!==void 0&&this.attenuationColor!==void 0&&this.attenuationColor.setHex(e.attenuationColor),e.anisotropy!==void 0&&(this.anisotropy=e.anisotropy),e.anisotropyRotation!==void 0&&(this.anisotropyRotation=e.anisotropyRotation),e.fog!==void 0&&(this.fog=e.fog),e.flatShading!==void 0&&(this.flatShading=e.flatShading),e.blending!==void 0&&(this.blending=e.blending),e.combine!==void 0&&(this.combine=e.combine),e.side!==void 0&&(this.side=e.side),e.shadowSide!==void 0&&(this.shadowSide=e.shadowSide),e.opacity!==void 0&&(this.opacity=e.opacity),e.transparent!==void 0&&(this.transparent=e.transparent),e.alphaTest!==void 0&&(this.alphaTest=e.alphaTest),e.alphaHash!==void 0&&(this.alphaHash=e.alphaHash),e.depthFunc!==void 0&&(this.depthFunc=e.depthFunc),e.depthTest!==void 0&&(this.depthTest=e.depthTest),e.depthWrite!==void 0&&(this.depthWrite=e.depthWrite),e.colorWrite!==void 0&&(this.colorWrite=e.colorWrite),e.clippingPlanes!==void 0&&(this.clippingPlanes=e.clippingPlanes.map(i=>new Yn().fromJSON(i))),e.clipIntersection!==void 0&&(this.clipIntersection=e.clipIntersection),e.clipShadows!==void 0&&(this.clipShadows=e.clipShadows),e.depthPacking!==void 0&&(this.depthPacking=e.depthPacking),e.blendSrc!==void 0&&(this.blendSrc=e.blendSrc),e.blendDst!==void 0&&(this.blendDst=e.blendDst),e.blendEquation!==void 0&&(this.blendEquation=e.blendEquation),e.blendSrcAlpha!==void 0&&(this.blendSrcAlpha=e.blendSrcAlpha),e.blendDstAlpha!==void 0&&(this.blendDstAlpha=e.blendDstAlpha),e.blendEquationAlpha!==void 0&&(this.blendEquationAlpha=e.blendEquationAlpha),e.blendColor!==void 0&&this.blendColor!==void 0&&this.blendColor.setHex(e.blendColor),e.blendAlpha!==void 0&&(this.blendAlpha=e.blendAlpha),e.stencilWriteMask!==void 0&&(this.stencilWriteMask=e.stencilWriteMask),e.stencilFunc!==void 0&&(this.stencilFunc=e.stencilFunc),e.stencilRef!==void 0&&(this.stencilRef=e.stencilRef),e.stencilFuncMask!==void 0&&(this.stencilFuncMask=e.stencilFuncMask),e.stencilFail!==void 0&&(this.stencilFail=e.stencilFail),e.stencilZFail!==void 0&&(this.stencilZFail=e.stencilZFail),e.stencilZPass!==void 0&&(this.stencilZPass=e.stencilZPass),e.stencilWrite!==void 0&&(this.stencilWrite=e.stencilWrite),e.wireframe!==void 0&&(this.wireframe=e.wireframe),e.wireframeLinewidth!==void 0&&(this.wireframeLinewidth=e.wireframeLinewidth),e.wireframeLinecap!==void 0&&(this.wireframeLinecap=e.wireframeLinecap),e.wireframeLinejoin!==void 0&&(this.wireframeLinejoin=e.wireframeLinejoin),e.rotation!==void 0&&(this.rotation=e.rotation),e.linewidth!==void 0&&(this.linewidth=e.linewidth),e.linecap!==void 0&&(this.linecap=e.linecap),e.linejoin!==void 0&&(this.linejoin=e.linejoin),e.dashSize!==void 0&&(this.dashSize=e.dashSize),e.gapSize!==void 0&&(this.gapSize=e.gapSize),e.scale!==void 0&&(this.scale=e.scale),e.polygonOffset!==void 0&&(this.polygonOffset=e.polygonOffset),e.polygonOffsetFactor!==void 0&&(this.polygonOffsetFactor=e.polygonOffsetFactor),e.polygonOffsetUnits!==void 0&&(this.polygonOffsetUnits=e.polygonOffsetUnits),e.dithering!==void 0&&(this.dithering=e.dithering),e.alphaToCoverage!==void 0&&(this.alphaToCoverage=e.alphaToCoverage),e.premultipliedAlpha!==void 0&&(this.premultipliedAlpha=e.premultipliedAlpha),e.forceSinglePass!==void 0&&(this.forceSinglePass=e.forceSinglePass),e.allowOverride!==void 0&&(this.allowOverride=e.allowOverride),e.visible!==void 0&&(this.visible=e.visible),e.toneMapped!==void 0&&(this.toneMapped=e.toneMapped),e.userData!==void 0&&(this.userData=e.userData),e.vertexColors!==void 0&&(typeof e.vertexColors=="number"?this.vertexColors=e.vertexColors>0:this.vertexColors=e.vertexColors),e.size!==void 0&&(this.size=e.size),e.sizeAttenuation!==void 0&&(this.sizeAttenuation=e.sizeAttenuation),e.map!==void 0&&(this.map=t[e.map]||null),e.matcap!==void 0&&(this.matcap=t[e.matcap]||null),e.alphaMap!==void 0&&(this.alphaMap=t[e.alphaMap]||null),e.bumpMap!==void 0&&(this.bumpMap=t[e.bumpMap]||null),e.bumpScale!==void 0&&(this.bumpScale=e.bumpScale),e.normalMap!==void 0&&(this.normalMap=t[e.normalMap]||null),e.normalMapType!==void 0&&(this.normalMapType=e.normalMapType),e.normalScale!==void 0){let i=e.normalScale;Array.isArray(i)===!1&&(i=[i,i]),this.normalScale=new Pe().fromArray(i)}return e.displacementMap!==void 0&&(this.displacementMap=t[e.displacementMap]||null),e.displacementScale!==void 0&&(this.displacementScale=e.displacementScale),e.displacementBias!==void 0&&(this.displacementBias=e.displacementBias),e.roughnessMap!==void 0&&(this.roughnessMap=t[e.roughnessMap]||null),e.metalnessMap!==void 0&&(this.metalnessMap=t[e.metalnessMap]||null),e.emissiveMap!==void 0&&(this.emissiveMap=t[e.emissiveMap]||null),e.emissiveIntensity!==void 0&&(this.emissiveIntensity=e.emissiveIntensity),e.specularMap!==void 0&&(this.specularMap=t[e.specularMap]||null),e.specularIntensityMap!==void 0&&(this.specularIntensityMap=t[e.specularIntensityMap]||null),e.specularColorMap!==void 0&&(this.specularColorMap=t[e.specularColorMap]||null),e.envMap!==void 0&&(this.envMap=t[e.envMap]||null),e.envMapRotation!==void 0&&this.envMapRotation.fromArray(e.envMapRotation),e.envMapIntensity!==void 0&&(this.envMapIntensity=e.envMapIntensity),e.reflectivity!==void 0&&(this.reflectivity=e.reflectivity),e.refractionRatio!==void 0&&(this.refractionRatio=e.refractionRatio),e.lightMap!==void 0&&(this.lightMap=t[e.lightMap]||null),e.lightMapIntensity!==void 0&&(this.lightMapIntensity=e.lightMapIntensity),e.aoMap!==void 0&&(this.aoMap=t[e.aoMap]||null),e.aoMapIntensity!==void 0&&(this.aoMapIntensity=e.aoMapIntensity),e.gradientMap!==void 0&&(this.gradientMap=t[e.gradientMap]||null),e.clearcoatMap!==void 0&&(this.clearcoatMap=t[e.clearcoatMap]||null),e.clearcoatRoughnessMap!==void 0&&(this.clearcoatRoughnessMap=t[e.clearcoatRoughnessMap]||null),e.clearcoatNormalMap!==void 0&&(this.clearcoatNormalMap=t[e.clearcoatNormalMap]||null),e.clearcoatNormalScale!==void 0&&(this.clearcoatNormalScale=new Pe().fromArray(e.clearcoatNormalScale)),e.iridescenceMap!==void 0&&(this.iridescenceMap=t[e.iridescenceMap]||null),e.iridescenceThicknessMap!==void 0&&(this.iridescenceThicknessMap=t[e.iridescenceThicknessMap]||null),e.transmissionMap!==void 0&&(this.transmissionMap=t[e.transmissionMap]||null),e.thicknessMap!==void 0&&(this.thicknessMap=t[e.thicknessMap]||null),e.anisotropyMap!==void 0&&(this.anisotropyMap=t[e.anisotropyMap]||null),e.sheenColorMap!==void 0&&(this.sheenColorMap=t[e.sheenColorMap]||null),e.sheenRoughnessMap!==void 0&&(this.sheenRoughnessMap=t[e.sheenRoughnessMap]||null),this}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}},Ta=class extends Tr{constructor(e){super(),this.isSpriteMaterial=!0,this.type="SpriteMaterial",this.color=new st(16777215),this.map=null,this.alphaMap=null,this.rotation=0,this.sizeAttenuation=!0,this.transparent=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.rotation=e.rotation,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},fa,sl=new F,ha=new F,pa=new F,ma=new Pe,ol=new Pe,tw=new Lt,Uf=new F,al=new F,kf=new F,rE=new Pe,_y=new Pe,sE=new Pe,Ml=class extends Ar{constructor(e=new Ta){if(super(),this.isSprite=!0,this.type="Sprite",fa===void 0){fa=new pn;let t=new Float32Array([-.5,-.5,0,0,0,.5,-.5,0,1,0,.5,.5,0,1,1,-.5,.5,0,0,1]),i=new yh(t,5);fa.setIndex([0,1,2,0,2,3]),fa.setAttribute("position",new bl(i,3,0,!1)),fa.setAttribute("uv",new bl(i,2,3,!1))}this.geometry=fa,this.material=e,this.center=new Pe(.5,.5),this.count=1}intersectsFrustum(e){return e.intersectsSprite(this)}raycast(e,t){e.camera===null&&Le('Sprite: "Raycaster.camera" needs to be set in order to raycast against sprites.'),ha.setFromMatrixScale(this.matrixWorld),tw.copy(e.camera.matrixWorld),this.modelViewMatrix.multiplyMatrices(e.camera.matrixWorldInverse,this.matrixWorld),pa.setFromMatrixPosition(this.modelViewMatrix),e.camera.isPerspectiveCamera&&this.material.sizeAttenuation===!1&&ha.multiplyScalar(-pa.z);let i=this.material.rotation,r,s;i!==0&&(s=Math.cos(i),r=Math.sin(i));let o=this.center;Bf(Uf.set(-.5,-.5,0),pa,o,ha,r,s),Bf(al.set(.5,-.5,0),pa,o,ha,r,s),Bf(kf.set(.5,.5,0),pa,o,ha,r,s),rE.set(0,0),_y.set(1,0),sE.set(1,1);let a=e.ray.intersectTriangle(Uf,al,kf,!1,sl);if(a===null&&(Bf(al.set(-.5,.5,0),pa,o,ha,r,s),_y.set(0,1),a=e.ray.intersectTriangle(Uf,kf,al,!1,sl),a===null))return;let c=e.ray.origin.distanceTo(sl);c<e.near||c>e.far||t.push({distance:c,point:sl.clone(),uv:Qi.getInterpolation(sl,Uf,al,kf,rE,_y,sE,new Pe),face:null,object:this})}copy(e,t){return super.copy(e,t),e.center!==void 0&&this.center.copy(e.center),this.material=e.material,this}};function Bf(n,e,t,i,r,s){ma.subVectors(n,t).addScalar(.5).multiply(i),r!==void 0?(ol.x=s*ma.x-r*ma.y,ol.y=r*ma.x+s*ma.y):ol.copy(ma),n.copy(e),n.x+=ol.x,n.y+=ol.y,n.applyMatrix4(tw)}var Mr=new F,xy=new F,Vf=new F,Hf=new F,eo=class{constructor(e=new F,t=new F(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Mr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Mr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Mr.copy(this.origin).addScaledVector(this.direction,t),Mr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){xy.copy(e).add(t).multiplyScalar(.5),Vf.copy(t).sub(e).normalize(),Hf.copy(this.origin).sub(xy);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Vf),a=Hf.dot(this.direction),c=-Hf.dot(Vf),l=Hf.lengthSq(),u=Math.abs(1-o*o),d,f,h,m;if(u>0)if(d=o*c-a,f=o*a-c,m=s*u,d>=0)if(f>=-m)if(f<=m){let y=1/u;d*=y,f*=y,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-m?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=m?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(xy).addScaledVector(Vf,f),h}intersectSphere(e,t){if(e.radius<0)return null;Mr.subVectors(e.center,this.origin);let i=Mr.dot(this.direction),r=Mr.dot(Mr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Mr)!==null}intersectTriangle(e,t,i,r,s){let o=this.origin,a=this.direction,c=a.x,l=a.y,u=a.z,d=e.x-o.x,f=e.y-o.y,h=e.z-o.z,m=t.x-o.x,y=t.y-o.y,g=t.z-o.z,p=i.x-o.x,b=i.y-o.y,A=i.z-o.z,S=Math.abs(c),M=Math.abs(l),E=Math.abs(u),I,_,T,R,O,C,V,D,U,$,B,X;if(S>=M&&S>=E?(T=c,C=d,U=m,X=p,c>=0?(I=l,_=u,R=f,O=h,V=y,D=g,$=b,B=A):(I=u,_=l,R=h,O=f,V=g,D=y,$=A,B=b)):M>=E?(T=l,C=f,U=y,X=b,l>=0?(I=u,_=c,R=h,O=d,V=g,D=m,$=A,B=p):(I=c,_=u,R=d,O=h,V=m,D=g,$=p,B=A)):(T=u,C=h,U=g,X=A,u>=0?(I=c,_=l,R=d,O=f,V=m,D=y,$=p,B=b):(I=l,_=c,R=f,O=d,V=y,D=m,$=b,B=p)),T===0)return null;let W=I/T,Q=_/T,ne=1/T,Ie=R-W*C,Te=O-Q*C,Ct=V-W*U,ot=D-Q*U,dt=$-W*X,Y=B-Q*X,ee=dt*ot-Y*Ct,_e=Ie*Y-Te*dt,Ve=Ct*Te-ot*Ie;if(r){if(ee<0||_e<0||Ve<0)return null}else if((ee<0||_e<0||Ve<0)&&(ee>0||_e>0||Ve>0))return null;let ve=ee+_e+Ve;if(ve===0)return null;let Ye=ne*(ee*C+_e*U+Ve*X);return(ve>0?Ye<0:Ye>0)?null:this.at(Ye/ve,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Cr=class extends Tr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new st(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new vl,this.combine=ky,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},oE=new Lt,Zs=new eo,zf=new Qs,aE=new F,Gf=new F,Wf=new F,jf=new F,Sy=new F,$f=new F,cE=new F,qf=new F,En=class extends Ar{constructor(e=new pn,t=new Cr){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){$f.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(Sy.fromBufferAttribute(d,e),o?$f.addScaledVector(Sy,u):$f.addScaledVector(Sy.sub(t),u))}t.add($f)}return t}intersectsFrustum(e){return e.intersectsObject(this)}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),zf.copy(i.boundingSphere),zf.applyMatrix4(s),Zs.copy(e.ray).recast(e.near),!(zf.containsPoint(Zs.origin)===!1&&(Zs.intersectSphere(zf,aE)===null||Zs.origin.distanceToSquared(aE)>(e.far-e.near)**2))&&(oE.copy(s).invert(),Zs.copy(e.ray).applyMatrix4(oE),!(i.boundingBox!==null&&Zs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Zs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let m=0,y=f.length;m<y;m++){let g=f[m],p=o[g.materialIndex],b=Math.max(g.start,h.start),A=Math.min(a.count,Math.min(g.start+g.count,h.start+h.count));for(let S=b,M=A;S<M;S+=3){let E=a.getX(S),I=a.getX(S+1),_=a.getX(S+2);r=Xf(this,p,e,i,l,u,d,E,I,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,h.start),y=Math.min(a.count,h.start+h.count);for(let g=m,p=y;g<p;g+=3){let b=a.getX(g),A=a.getX(g+1),S=a.getX(g+2);r=Xf(this,o,e,i,l,u,d,b,A,S),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let m=0,y=f.length;m<y;m++){let g=f[m],p=o[g.materialIndex],b=Math.max(g.start,h.start),A=Math.min(c.count,Math.min(g.start+g.count,h.start+h.count));for(let S=b,M=A;S<M;S+=3){let E=S,I=S+1,_=S+2;r=Xf(this,p,e,i,l,u,d,E,I,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,h.start),y=Math.min(c.count,h.start+h.count);for(let g=m,p=y;g<p;g+=3){let b=g,A=g+1,S=g+2;r=Xf(this,o,e,i,l,u,d,b,A,S),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}}};function tP(n,e,t,i,r,s,o,a){let c;if(e.side===Pn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===us,a),c===null)return null;qf.copy(a),qf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(qf);return l<t.near||l>t.far?null:{distance:l,point:qf.clone(),object:n}}function Xf(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Gf),n.getVertexPosition(c,Wf),n.getVertexPosition(l,jf);let u=tP(n,e,t,i,Gf,Wf,jf,cE);if(u){let d=new F;Qi.getBarycoord(cE,Gf,Wf,jf,d),r&&(u.uv=Qi.getInterpolatedAttribute(r,a,c,l,d,new Pe)),s&&(u.uv1=Qi.getInterpolatedAttribute(s,a,c,l,d,new Pe)),o&&(u.normal=Qi.getInterpolatedAttribute(o,a,c,l,d,new F),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new F,materialIndex:0};Qi.getNormal(Gf,Wf,jf,f.normal),u.face=f,u.barycoord=d}return u}var _h=class extends ir{constructor(e=null,t=1,i=1,r,s,o,a,c,l=sn,u=sn,d,f){super(null,o,a,c,l,u,r,s,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ks=new Qs,nP=new Pe(.5,.5),Yf=new F,El=class{constructor(e=new Yn,t=new Yn,i=new Yn,r=new Yn,s=new Yn,o=new Yn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Ri,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],f=s[6],h=s[7],m=s[8],y=s[9],g=s[10],p=s[11],b=s[12],A=s[13],S=s[14],M=s[15];if(r[0].setComponents(l-o,h-u,p-m,M-b).normalize(),r[1].setComponents(l+o,h+u,p+m,M+b).normalize(),r[2].setComponents(l+a,h+d,p+y,M+A).normalize(),r[3].setComponents(l-a,h-d,p-y,M-A).normalize(),i)r[4].setComponents(c,f,g,S).normalize(),r[5].setComponents(l-c,h-f,p-g,M-S).normalize();else if(r[4].setComponents(l-c,h-f,p-g,M-S).normalize(),t===Ri)r[5].setComponents(l+c,h+f,p+g,M+S).normalize();else if(t===hl)r[5].setComponents(c,f,g,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Ks.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Ks.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Ks)}intersectsSprite(e){Ks.center.set(0,0,0);let t=nP.distanceTo(e.center);return Ks.radius=.7071067811865476+t,Ks.applyMatrix4(e.matrixWorld),this.intersectsSphere(Ks)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Yf.x=r.normal.x>0?e.max.x:e.min.x,Yf.y=r.normal.y>0?e.max.y:e.min.y,Yf.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Yf)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var to=class extends Tr{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new st(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},xh=new F,Sh=new F,lE=new Lt,cl=new eo,Zf=new Qs,by=new F,uE=new F,bh=class extends Ar{constructor(e=new pn,t=new to){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)xh.fromBufferAttribute(t,r-1),Sh.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=xh.distanceTo(Sh);e.setAttribute("lineDistance",new jt(i,1))}else Re("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}intersectsFrustum(e){return e.intersectsObject(this)}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Zf.copy(i.boundingSphere),Zf.applyMatrix4(r),Zf.radius+=s,e.ray.intersectsSphere(Zf)===!1)return;lE.copy(r).invert(),cl.copy(e.ray).applyMatrix4(lE);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),m=Math.min(u.count,o.start+o.count);for(let y=h,g=m-1;y<g;y+=l){let p=u.getX(y),b=u.getX(y+1),A=Kf(this,e,cl,c,p,b,y);A&&t.push(A)}if(this.isLineLoop){let y=u.getX(m-1),g=u.getX(h),p=Kf(this,e,cl,c,y,g,m-1);p&&t.push(p)}}else{let h=Math.max(0,o.start),m=Math.min(f.count,o.start+o.count);for(let y=h,g=m-1;y<g;y+=l){let p=Kf(this,e,cl,c,y,y+1,y);p&&t.push(p)}if(this.isLineLoop){let y=Kf(this,e,cl,c,m-1,h,m-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function Kf(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(xh.fromBufferAttribute(a,r),Sh.fromBufferAttribute(a,s),t.distanceSqToSegment(xh,Sh,by,uE)>i)return;by.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(by);if(!(l<e.near||l>e.far))return{distance:l,point:uE.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var dE=new F,fE=new F,Ca=class extends bh{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)dE.fromBufferAttribute(t,r),fE.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+dE.distanceTo(fE);e.setAttribute("lineDistance",new jt(i,1))}else Re("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var wl=class extends ir{constructor(e=[],t=ds,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},Tl=class extends ir{constructor(e,t,i,r,s,o,a,c,l){super(e,t,i,r,s,o,a,c,l),this.isCanvasTexture=!0,this.needsUpdate=!0}};var is=class extends ir{constructor(e,t,i=Oi,r,s,o,a=sn,c=sn,l,u=tr,d=1){if(u!==tr&&u!==hs)throw new Error("THREE.DepthTexture: format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new Ea(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return t.compareFunction=this.compareFunction,t}},Mh=class extends is{constructor(e,t=Oi,i=ds,r,s,o=sn,a=sn,c,l=tr){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Cl=class extends ir{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},rs=class n extends pn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;m("z","y","x",-1,-1,i,t,e,o,s,0),m("z","y","x",1,-1,i,t,-e,o,s,1),m("x","z","y",1,1,e,i,t,r,o,2),m("x","z","y",1,-1,e,i,-t,r,o,3),m("x","y","z",1,-1,e,t,i,r,s,4),m("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new jt(l,3)),this.setAttribute("normal",new jt(u,3)),this.setAttribute("uv",new jt(d,2));function m(y,g,p,b,A,S,M,E,I,_,T){let R=S/I,O=M/_,C=S/2,V=M/2,D=E/2,U=I+1,$=_+1,B=0,X=0,W=new F;for(let Q=0;Q<$;Q++){let ne=Q*O-V;for(let Ie=0;Ie<U;Ie++){let Te=Ie*R-C;W[y]=Te*b,W[g]=ne*A,W[p]=D,l.push(W.x,W.y,W.z),W[y]=0,W[g]=0,W[p]=E>0?1:-1,u.push(W.x,W.y,W.z),d.push(Ie/I),d.push(1-Q/_),B+=1}}for(let Q=0;Q<_;Q++)for(let ne=0;ne<I;ne++){let Ie=f+ne+U*Q,Te=f+ne+U*(Q+1),Ct=f+(ne+1)+U*(Q+1),ot=f+(ne+1)+U*Q;c.push(Ie,Te,ot),c.push(Te,Ct,ot),X+=6}a.addGroup(h,X,T),h+=X,f+=B}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Jf=new F,Qf=new F,My=new F,eh=new Qi,Il=class extends pn{constructor(e=null,t=1){if(super(),this.type="EdgesGeometry",this.parameters={geometry:e,thresholdAngle:t},e!==null){let r=Math.pow(10,4),s=Math.cos(_a*t),o=e.getIndex(),a=e.getAttribute("position"),c=o?o.count:a.count,l=[0,0,0],u=["a","b","c"],d=new Array(3),f={},h=[];for(let m=0;m<c;m+=3){o?(l[0]=o.getX(m),l[1]=o.getX(m+1),l[2]=o.getX(m+2)):(l[0]=m,l[1]=m+1,l[2]=m+2);let{a:y,b:g,c:p}=eh;if(y.fromBufferAttribute(a,l[0]),g.fromBufferAttribute(a,l[1]),p.fromBufferAttribute(a,l[2]),eh.getNormal(My),d[0]=`${Math.round(y.x*r)},${Math.round(y.y*r)},${Math.round(y.z*r)}`,d[1]=`${Math.round(g.x*r)},${Math.round(g.y*r)},${Math.round(g.z*r)}`,d[2]=`${Math.round(p.x*r)},${Math.round(p.y*r)},${Math.round(p.z*r)}`,!(d[0]===d[1]||d[1]===d[2]||d[2]===d[0]))for(let b=0;b<3;b++){let A=(b+1)%3,S=d[b],M=d[A],E=eh[u[b]],I=eh[u[A]],_=`${S}_${M}`,T=`${M}_${S}`;T in f&&f[T]?(My.dot(f[T].normal)<=s&&(h.push(E.x,E.y,E.z),h.push(I.x,I.y,I.z)),f[T]=null):_ in f||(f[_]={index0:l[b],index1:l[A],normal:My.clone()})}}for(let m in f)if(f[m]){let{index0:y,index1:g}=f[m];Jf.fromBufferAttribute(a,y),Qf.fromBufferAttribute(a,g),h.push(Jf.x,Jf.y,Jf.z),h.push(Qf.x,Qf.y,Qf.z)}this.setAttribute("position",new jt(h,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};var no=class n extends pn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],m=[],y=[],g=[];for(let p=0;p<u;p++){let b=p*f-o;for(let A=0;A<l;A++){let S=A*d-s;m.push(S,-b,0),y.push(0,0,1),g.push(A/a),g.push(1-p/c)}}for(let p=0;p<c;p++)for(let b=0;b<a;b++){let A=b+l*p,S=b+l*(p+1),M=b+1+l*(p+1),E=b+1+l*p;h.push(A,S,E),h.push(S,M,E)}this.setIndex(h),this.setAttribute("position",new jt(m,3)),this.setAttribute("normal",new jt(y,3)),this.setAttribute("uv",new jt(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var Al=class n extends pn{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new F,f=new F,h=[],m=[],y=[],g=[];for(let p=0;p<=i;p++){let b=[],A=p/i,S=o+A*a,M=e*Math.cos(S),E=Math.sqrt(e*e-M*M),I=0;p===0&&o===0?I=.5/t:p===i&&c===Math.PI&&(I=-.5/t);for(let _=0;_<=t;_++){let T=_/t,R=r+T*s;d.x=-E*Math.cos(R),d.y=M,d.z=E*Math.sin(R),m.push(d.x,d.y,d.z),f.copy(d).normalize(),y.push(f.x,f.y,f.z),g.push(T+I,1-A),b.push(l++)}u.push(b)}for(let p=0;p<i;p++)for(let b=0;b<t;b++){let A=u[p][b+1],S=u[p][b],M=u[p+1][b],E=u[p+1][b+1];(p!==0||o>0)&&h.push(A,S,E),(p!==i-1||c<Math.PI)&&h.push(S,M,E)}this.setIndex(h),this.setAttribute("position",new jt(m,3)),this.setAttribute("normal",new jt(y,3)),this.setAttribute("uv",new jt(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};function so(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(hE(r))r.isRenderTargetTexture?(Re("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(hE(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function wn(n){let e={};for(let t=0;t<n.length;t++){let i=so(n[t]);for(let r in i)e[r]=i[r]}return e}function hE(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function iP(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function r_(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:rt.workingColorSpace}var nw={clone:so,merge:wn},rP=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,sP=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Kn=class extends Tr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=rP,this.fragmentShader=sP,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=so(e.uniforms),this.uniformsGroups=iP(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}fromJSON(e,t){if(super.fromJSON(e,t),e.uniforms!==void 0)for(let i in e.uniforms){let r=e.uniforms[i];switch(this.uniforms[i]={},r.type){case"t":this.uniforms[i].value=t[r.value]||null;break;case"c":this.uniforms[i].value=new st().setHex(r.value);break;case"v2":this.uniforms[i].value=new Pe().fromArray(r.value);break;case"v3":this.uniforms[i].value=new F().fromArray(r.value);break;case"v4":this.uniforms[i].value=new Ot().fromArray(r.value);break;case"m3":this.uniforms[i].value=new Be().fromArray(r.value);break;case"m4":this.uniforms[i].value=new Lt().fromArray(r.value);break;default:this.uniforms[i].value=r.value}}if(e.defines!==void 0&&(this.defines=e.defines),e.vertexShader!==void 0&&(this.vertexShader=e.vertexShader),e.fragmentShader!==void 0&&(this.fragmentShader=e.fragmentShader),e.glslVersion!==void 0&&(this.glslVersion=e.glslVersion),e.extensions!==void 0)for(let i in e.extensions)this.extensions[i]=e.extensions[i];return e.lights!==void 0&&(this.lights=e.lights),e.clipping!==void 0&&(this.clipping=e.clipping),this}},Eh=class extends Kn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var wh=class extends Tr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=HE,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Th=class extends Tr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function ga(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function Ey(n){return n!==void 0&&n.inTangents!==void 0&&n.outTangents!==void 0}var ss=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("THREE.Interpolant: Call to abstract method.")}intervalChanged_(){}},Ch=class extends ss{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Cy,endingEnd:Cy}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Iy:s=e,a=2*t-i;break;case Ay:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Iy:o=e,c=2*i-t;break;case Ay:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,m=(i-t)/(r-t),y=m*m,g=y*m,p=-f*g+2*f*y-f*m,b=(1+f)*g+(-1.5-2*f)*y+(-.5+f)*m+1,A=(-1-h)*g+(1.5+h)*y+.5*m,S=h*g-h*y;for(let M=0;M!==a;++M)s[M]=p*o[u+M]+b*o[l+M]+A*o[c+M]+S*o[d+M];return s}},Ih=class extends ss{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},Ah=class extends ss{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Dh=class extends ss{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.inTangents,d=this.outTangents;if(!u||!d){let m=(i-t)/(r-t),y=1-m;for(let g=0;g!==a;++g)s[g]=o[l+g]*y+o[c+g]*m;return s}let f=a*2,h=e-1;for(let m=0;m!==a;++m){let y=o[l+m],g=o[c+m],p=h*f+m*2,b=d[p],A=d[p+1],S=e*f+m*2,M=u[S],E=u[S+1],I=aP(i,t,b,M,r);s[m]=iw(I,y,A,E,g)}return s}};function iw(n,e,t,i,r){let s=1-n;return s*s*s*e+3*s*s*n*t+3*s*n*n*i+n*n*n*r}function oP(n,e,t,i,r){let s=1-n;return 3*s*s*(t-e)+6*s*n*(i-t)+3*n*n*(r-i)}function aP(n,e,t,i,r){let s=(n-e)/(r-e);for(let o=0;o<8;o++){let a=iw(s,e,t,i,r)-n;if(Math.abs(a)<1e-10)break;let c=oP(s,e,t,i,r);if(Math.abs(c)<1e-10)break;s=Math.max(0,Math.min(1,s-a/c))}return s}var Jn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=ga(t,this.TimeBufferType),this.values=ga(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:ga(e.times,Array),values:ga(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r),Ey(e.settings)&&(i.settings={inTangents:ga(e.settings.inTangents,Array),outTangents:ga(e.settings.outTangents,Array)})}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Ah(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Ih(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Ch(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Dh(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.inTangents=this.settings.inTangents,t.outTangents=this.settings.outTangents),t}setInterpolation(e){let t;switch(e){case ul:t=this.InterpolantFactoryMethodDiscrete;break;case ph:t=this.InterpolantFactoryMethodLinear;break;case ih:t=this.InterpolantFactoryMethodSmooth;break;case Ty:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Re("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return ul;case this.InterpolantFactoryMethodLinear:return ph;case this.InterpolantFactoryMethodSmooth:return ih;case this.InterpolantFactoryMethodBezier:return Ty}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e;Ey(this.settings)&&(pE(this.settings.inTangents,e),pE(this.settings.outTangents,e))}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Le("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Le("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Le("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Le("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&b1(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Le("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===ih,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let m=0;m!==i;++m){let y=t[d+m];if(y!==t[f+m]||y!==t[h+m]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,Ey(this.settings)&&(r.settings={inTangents:this.settings.inTangents.slice(),outTangents:this.settings.outTangents.slice()}),r}};function pE(n,e){for(let t=0,i=n.length;t!==i;t+=2)n[t]*=e}Jn.prototype.ValueTypeName="";Jn.prototype.TimeBufferType=Float32Array;Jn.prototype.ValueBufferType=Float32Array;Jn.prototype.DefaultInterpolation=ph;var os=class extends Jn{constructor(e,t,i){super(e,t,i)}};os.prototype.ValueTypeName="bool";os.prototype.ValueBufferType=Array;os.prototype.DefaultInterpolation=ul;os.prototype.InterpolantFactoryMethodLinear=void 0;os.prototype.InterpolantFactoryMethodSmooth=void 0;var Rh=class extends Jn{constructor(e,t,i,r){super(e,t,i,r)}};Rh.prototype.ValueTypeName="color";var Nh=class extends Jn{constructor(e,t,i,r){super(e,t,i,r)}};Nh.prototype.ValueTypeName="number";var Ph=class extends ss{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Zn.slerpFlat(s,0,o,l-a,o,l,c);return s}},Dl=class extends Jn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Ph(this.times,this.values,this.getValueSize(),e)}};Dl.prototype.ValueTypeName="quaternion";Dl.prototype.InterpolantFactoryMethodSmooth=void 0;var as=class extends Jn{constructor(e,t,i){super(e,t,i)}};as.prototype.ValueTypeName="string";as.prototype.ValueBufferType=Array;as.prototype.DefaultInterpolation=ul;as.prototype.InterpolantFactoryMethodLinear=void 0;as.prototype.InterpolantFactoryMethodSmooth=void 0;var Lh=class extends Jn{constructor(e,t,i,r){super(e,t,i,r)}};Lh.prototype.ValueTypeName="vector";var th=new F,nh=new Zn,Ji=new F,Rl=class extends Ar{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Lt,this.projectionMatrix=new Lt,this.projectionMatrixInverse=new Lt,this.coordinateSystem=Ri,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(th,nh,Ji),Ji.x===1&&Ji.y===1&&Ji.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(th,nh,Ji.set(1,1,1)).invert()}updateWorldMatrix(e,t,i=!1){super.updateWorldMatrix(e,t,i),this.matrixWorld.decompose(th,nh,Ji),Ji.x===1&&Ji.y===1&&Ji.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(th,nh,Ji.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},ts=new F,mE=new Pe,gE=new Pe,Mn=class extends Rl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Ma*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(_a*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Ma*2*Math.atan(Math.tan(_a*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){ts.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(ts.x,ts.y).multiplyScalar(-e/ts.z),ts.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(ts.x,ts.y).multiplyScalar(-e/ts.z)}getViewSize(e,t){return this.getViewBounds(e,mE,gE),t.subVectors(gE,mE)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(_a*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Nl=class extends Rl{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var va=-90,ya=1,Oh=class extends Ar{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Mn(va,ya,e,t);r.layers=this.layers,this.add(r);let s=new Mn(va,ya,e,t);s.layers=this.layers,this.add(s);let o=new Mn(va,ya,e,t);o.layers=this.layers,this.add(o);let a=new Mn(va,ya,e,t);a.layers=this.layers,this.add(a);let c=new Mn(va,ya,e,t);c.layers=this.layers,this.add(c);let l=new Mn(va,ya,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===Ri)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===hl)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),m=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let g=!1;e.isWebGLRenderer===!0?g=e.state.buffers.depth.getReversed():g=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=m,i.texture.needsPMREMUpdate=!0}},Fh=class extends Mn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var s_="\\[\\]\\.:\\/",cP=new RegExp("["+s_+"]","g"),o_="[^"+s_+"]",lP="[^"+s_.replace("\\.","")+"]",uP=/((?:WC+[\/:])*)/.source.replace("WC",o_),dP=/(WCOD+)?/.source.replace("WCOD",lP),fP=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",o_),hP=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",o_),pP=new RegExp("^"+uP+dP+fP+hP+"$"),mP=["material","materials","bones","map"],Dy=class{constructor(e,t,i){let r=i||kt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},kt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(cP,"")}static parseTrackName(t){let i=pP.exec(t);if(i===null)throw new Error("THREE.PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);mP.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("THREE.PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Re("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Le("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Le("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Le("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Le("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Le("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Le("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Le("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Le("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Le("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Le("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Dy,n})();kt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};kt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};kt.prototype.GetterByBindingType=[kt.prototype._getValue_direct,kt.prototype._getValue_array,kt.prototype._getValue_arrayElement,kt.prototype._getValue_toArray];kt.prototype.SetterByBindingTypeAndVersioning=[[kt.prototype._setValue_direct,kt.prototype._setValue_direct_setNeedsUpdate,kt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[kt.prototype._setValue_array,kt.prototype._setValue_array_setNeedsUpdate,kt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[kt.prototype._setValue_arrayElement,kt.prototype._setValue_arrayElement_setNeedsUpdate,kt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[kt.prototype._setValue_fromArray,kt.prototype._setValue_fromArray_setNeedsUpdate,kt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var k9=new Float32Array(1);var Ia=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=Ke(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(Ke(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var Ry=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};var Pl=class extends Pi{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function a_(n,e,t,i){let r=gP(i);switch(t){case Zy:return n*e;case Jy:return n*e/r.components*r.byteLength;case Gh:return n*e/r.components*r.byteLength;case ps:return n*e*2/r.components*r.byteLength;case Wh:return n*e*2/r.components*r.byteLength;case Ky:return n*e*3/r.components*r.byteLength;case di:return n*e*4/r.components*r.byteLength;case jh:return n*e*4/r.components*r.byteLength;case Ul:case kl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Bl:case Vl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case qh:case Yh:return Math.max(n,16)*Math.max(e,8)/4;case $h:case Xh:return Math.max(n,8)*Math.max(e,8)/2;case Zh:case Kh:case Qh:case ep:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Jh:case Hl:case tp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case np:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ip:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case rp:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case sp:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case op:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case ap:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case cp:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case lp:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case up:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case dp:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case fp:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case hp:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case pp:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case mp:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case gp:case vp:case yp:return Math.ceil(n/4)*Math.ceil(e/4)*16;case _p:case xp:return Math.ceil(n/4)*Math.ceil(e/4)*8;case zl:case Sp:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function gP(n){switch(n){case ei:case $y:return{byteLength:1,components:1};case Ra:case qy:case Ui:return{byteLength:2,components:1};case Hh:case zh:return{byteLength:2,components:4};case Oi:case Vh:case Fi:return{byteLength:4,components:1};case Xy:case Yy:return{byteLength:4,components:3}}throw new Error(`THREE.TextureUtils: Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"186"}}));typeof window<"u"&&(window.__THREE__?Re("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="186");function ww(){let n=null,e=!1,t=null,i=null;function r(s,o){i=n.requestAnimationFrame(r),t(s,o)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function yP(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,m)=>h.start-m.start);let f=0;for(let h=1;h<d.length;h++){let m=d[f],y=d[h];y.start<=m.start+m.count+1?m.count=Math.max(m.count,y.start+y.count-m.start):(++f,d[f]=y)}d.length=f+1;for(let h=0,m=d.length;h<m;h++){let y=d[h];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var _P=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,xP=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,SP=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,bP=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,MP=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,EP=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,wP=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,TP=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,CP=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,IP=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,AP=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,DP=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,RP=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,NP=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,PP=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,LP=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,OP=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,FP=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,UP=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,kP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,BP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,VP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,HP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,zP=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
#define inverseTransformDirection transformDirectionByInverseViewMatrix
vec3 transformNormalByInverseViewMatrix( in vec3 normal, in mat4 viewMatrix ) {
	return normalize( ( vec4( normal, 0.0 ) * viewMatrix ).xyz );
}
vec3 transformDirectionByInverseViewMatrix( in vec3 dir, in mat4 viewMatrix ) {
	return normalize( ( vec4( dir, 0.0 ) * viewMatrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,GP=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,WP=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
#endif`,jP=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,$P=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,qP=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,XP=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,YP="gl_FragColor = linearToOutputTexel( gl_FragColor );",ZP=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,KP=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,JP=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,QP=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,eL=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,tL=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = transformNormalByInverseViewMatrix( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,nL=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,iL=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,rL=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,sL=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,oL=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,aL=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,cL=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,lL=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,uL=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_SUN_LIGHTS > 0
	struct SunLight {
		vec3 direction;
		vec3 color;
	};
	uniform SunLight sunLights[ NUM_SUN_LIGHTS ];
	void getSunLightInfo( const in SunLight sunLight, out IncidentLight light ) {
		light.color = sunLight.color;
		light.direction = sunLight.direction;
		light.visible = true;
	}
#endif
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,dL=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = transformNormalByInverseViewMatrix( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = transformDirectionByInverseViewMatrix( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_RETROREFLECTION
		vec3 getIBLRetroRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 retroVec = normalize( mix( viewDir, normal, pow4( roughness ) ) );
				retroVec = transformDirectionByInverseViewMatrix( retroVec, viewMatrix );
				vec4 envMapColor = textureCubeUV( envMap, envMapRotation * retroVec, roughness );
				return envMapColor.rgb * envMapIntensity;
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
		#ifdef USE_RETROREFLECTION
			vec3 getIBLAnisotropyRetroRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
				#ifdef ENVMAP_TYPE_CUBE_UV
					vec3 bentNormal = cross( bitangent, viewDir );
					bentNormal = normalize( cross( bentNormal, bitangent ) );
					bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
					return getIBLRetroRadiance( viewDir, bentNormal, roughness );
				#else
					return vec3( 0.0 );
				#endif
			}
		#endif
	#endif
#endif`,fL=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,hL=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,pL=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,mL=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,gL=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_RETROREFLECTION
	material.retroreflectivity = retroreflectivity;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,vL=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	vec2 dfg;
	vec3 multiScatteringCompensation;
	#ifdef USE_RETROREFLECTION
		float retroreflectivity;
	#endif
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0Dielectric;
		vec3 iridescenceF0Metallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec2 fab, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec2 fab, const in vec3 specularColor, const in float specularF90, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	vec3 specularBRDF = BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	#ifdef USE_RETROREFLECTION
		vec3 retroViewDir = reflect( - geometryViewDir, geometryNormal );
		vec3 retroSpecularBRDF = BRDF_GGX( directLight.direction, retroViewDir, geometryNormal, material );
		specularBRDF = mix( specularBRDF, retroSpecularBRDF, saturate( material.retroreflectivity ) );
	#endif
	reflectedLight.directSpecular += irradiance * specularBRDF * material.multiScatteringCompensation;
	vec3 halfDir = normalize( directLight.direction + geometryViewDir );
	float dotVH = saturate( dot( geometryViewDir, halfDir ) );
	vec3 F = F_Schlick( material.specularColor, material.specularF90, dotVH );
	#ifdef USE_RETROREFLECTION
		vec3 retroHalfDir = normalize( directLight.direction + retroViewDir );
		float dotRetroVH = saturate( dot( retroViewDir, retroHalfDir ) );
		vec3 retroF = F_Schlick( material.specularColor, material.specularF90, dotRetroVH );
		F = mix( F, retroF, saturate( material.retroreflectivity ) );
	#endif
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution ) * ( 1.0 - F );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( material.dfg, material.specularColor, material.specularF90, material.iridescence, material.iridescenceF0Dielectric, singleScattering, multiScattering );
	#else
		computeMultiscattering( material.dfg, material.specularColor, material.specularF90, singleScattering, multiScattering );
	#endif
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution ) * ( 1.0 - singleScattering - multiScattering );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		sheenSpecularIndirect += irradiance * material.sheenColor * sheenAlbedo * RECIPROCAL_PI;
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( material.dfg, material.specularColor, material.specularF90, material.iridescence, material.iridescenceF0Dielectric, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( material.dfg, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceF0Metallic, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( material.dfg, material.specularColor, material.specularF90, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( material.dfg, material.diffuseColor, material.specularF90, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,yL=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		vec3 iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		vec3 iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( iridescenceFresnelDielectric, iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0Dielectric = Schlick_to_F0( iridescenceFresnelDielectric, 1.0, dotNVi );
		material.iridescenceF0Metallic = Schlick_to_F0( iridescenceFresnelMetallic, 1.0, dotNVi );
	}
#endif
#ifdef STANDARD
	float dotNVms = saturate( dot( geometryNormal, geometryViewDir ) );
	material.dfg = texture2D( dfgLUT, vec2( material.roughness, dotNVms ) ).rg;
	#if ( NUM_SUN_LIGHTS > 0 || NUM_DIR_LIGHTS > 0 || NUM_POINT_LIGHTS > 0 || NUM_SPOT_LIGHTS > 0 )
		float EssMs = material.dfg.x + material.dfg.y;
		material.multiScatteringCompensation = 1.0 + material.specularColorBlended * ( 1.0 / EssMs - 1.0 );
	#endif
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SUN_LIGHTS > 0 ) && defined( RE_Direct )
	SunLight sunLight;
	#if defined( USE_SHADOWMAP ) && NUM_SUN_LIGHT_SHADOWS > 0
	SunLightShadow sunLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SUN_LIGHTS; i ++ ) {
		sunLight = sunLights[ i ];
		getSunLightInfo( sunLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SUN_LIGHT_SHADOWS )
		sunLightShadow = sunLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getSunShadow( sunShadowMap[ i ], sunLightShadow, UNROLLED_LOOP_INDEX ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = transformNormalByInverseViewMatrix( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,_L=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		vec3 iblRadiance = getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		vec3 iblRadiance = getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_RETROREFLECTION
		#ifdef USE_ANISOTROPY
			vec3 retroIBLRadiance = getIBLAnisotropyRetroRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
		#else
			vec3 retroIBLRadiance = getIBLRetroRadiance( geometryViewDir, geometryNormal, material.roughness );
		#endif
		iblRadiance = mix( iblRadiance, retroIBLRadiance, saturate( material.retroreflectivity ) );
	#endif
	radiance += iblRadiance;
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,xL=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,SL=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,bL=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,ML=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,EL=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,wL=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,TL=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,CL=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,IL=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,AL=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,DL=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,RL=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,NL=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,PL=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,LL=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,OL=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,FL=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,UL=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#ifdef DOUBLE_SIDED
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#ifdef DOUBLE_SIDED
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,kL=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,BL=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,VL=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,HL=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
		#ifdef FLIP_SIDED
			vBitangent = - vBitangent;
		#endif
	#endif
#endif`,zL=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,GL=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,WL=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,jL=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,$L=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,qL=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,XL=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,YL=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,ZL=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,KL=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,JL=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,QL=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,eO=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,tO=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
		#define SUN_LIGHT_CASCADES 2
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow sunShadowMap[ NUM_SUN_LIGHT_SHADOWS ];
		#else
			uniform sampler2D sunShadowMap[ NUM_SUN_LIGHT_SHADOWS ];
		#endif
		uniform mat4 sunShadowMatrix[ NUM_SUN_LIGHT_SHADOWS * SUN_LIGHT_CASCADES ];
		uniform vec4 sunShadowCascade[ NUM_SUN_LIGHT_SHADOWS * SUN_LIGHT_CASCADES ];
		varying vec4 vSunShadowWorldPosition;
		varying vec3 vSunShadowWorldNormal;
		struct SunLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SunLightShadow sunLightShadows[ NUM_SUN_LIGHT_SHADOWS ];
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_SUN_LIGHT_SHADOWS > 0
		float getSunShadow(
			#if defined( SHADOWMAP_TYPE_PCF )
				sampler2DShadow shadowMap,
			#else
				sampler2D shadowMap,
			#endif
			SunLightShadow sunLightShadow,
			int shadowIndex
		) {
			vec4 shadowWorldPosition = vec4( vSunShadowWorldPosition.xyz + vSunShadowWorldNormal * sunLightShadow.shadowNormalBias, 1.0 );
			float viewDepth = vSunShadowWorldPosition.w;
			int cascadeOffset = shadowIndex * SUN_LIGHT_CASCADES;
			float shadow = 1.0;
			for ( int i = SUN_LIGHT_CASCADES - 1; i >= 0; i -- ) {
				vec4 cascade = sunShadowCascade[ cascadeOffset + i ];
				if ( viewDepth >= cascade.x && viewDepth < cascade.y ) {
					float cascadeShadow = getShadow(
						shadowMap,
						sunLightShadow.shadowMapSize,
						sunLightShadow.shadowIntensity,
						sunLightShadow.shadowBias,
						sunLightShadow.shadowRadius,
						sunShadowMatrix[ cascadeOffset + i ] * shadowWorldPosition
					);
					shadow = mix( cascadeShadow, shadow, smoothstep( cascade.z, cascade.y, viewDepth ) );
				}
			}
			return shadow;
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,nO=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
		varying vec4 vSunShadowWorldPosition;
		varying vec3 vSunShadowWorldNormal;
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,iO=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_SUN_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = transformNormalByInverseViewMatrix( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_SUN_LIGHT_SHADOWS > 0
		vSunShadowWorldPosition = vec4( worldPosition.xyz, - mvPosition.z );
		vSunShadowWorldNormal = shadowWorldNormal;
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,rO=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_SUN_LIGHT_SHADOWS > 0
	SunLightShadow sunLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SUN_LIGHT_SHADOWS; i ++ ) {
		sunLight = sunLightShadows[ i ];
		shadow *= receiveShadow ? getSunShadow( sunShadowMap[ i ], sunLight, UNROLLED_LOOP_INDEX ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,sO=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,oO=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,aO=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,cO=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,lO=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,uO=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,dO=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,fO=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,hO=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = transformNormalByInverseViewMatrix( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,pO=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,mO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,gO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,vO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,yO=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,_O=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,xO=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,SO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,bO=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,MO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,EO=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,wO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,TO=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,CO=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,IO=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,AO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,DO=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,RO=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,NO=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,PO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,LO=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,OO=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,FO=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,UO=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,kO=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,BO=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,VO=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,HO=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,zO=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,GO=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,WO=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_RETROREFLECTION
	uniform float retroreflectivity;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,jO=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,$O=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,qO=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,XO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,YO=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,ZO=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,KO=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,JO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,qe={alphahash_fragment:_P,alphahash_pars_fragment:xP,alphamap_fragment:SP,alphamap_pars_fragment:bP,alphatest_fragment:MP,alphatest_pars_fragment:EP,aomap_fragment:wP,aomap_pars_fragment:TP,batching_pars_vertex:CP,batching_vertex:IP,begin_vertex:AP,beginnormal_vertex:DP,bsdfs:RP,iridescence_fragment:NP,bumpmap_pars_fragment:PP,clipping_planes_fragment:LP,clipping_planes_pars_fragment:OP,clipping_planes_pars_vertex:FP,clipping_planes_vertex:UP,color_fragment:kP,color_pars_fragment:BP,color_pars_vertex:VP,color_vertex:HP,common:zP,cube_uv_reflection_fragment:GP,defaultnormal_vertex:WP,displacementmap_pars_vertex:jP,displacementmap_vertex:$P,emissivemap_fragment:qP,emissivemap_pars_fragment:XP,colorspace_fragment:YP,colorspace_pars_fragment:ZP,envmap_fragment:KP,envmap_common_pars_fragment:JP,envmap_pars_fragment:QP,envmap_pars_vertex:eL,envmap_physical_pars_fragment:dL,envmap_vertex:tL,fog_vertex:nL,fog_pars_vertex:iL,fog_fragment:rL,fog_pars_fragment:sL,gradientmap_pars_fragment:oL,lightmap_pars_fragment:aL,lights_lambert_fragment:cL,lights_lambert_pars_fragment:lL,lights_pars_begin:uL,lights_toon_fragment:fL,lights_toon_pars_fragment:hL,lights_phong_fragment:pL,lights_phong_pars_fragment:mL,lights_physical_fragment:gL,lights_physical_pars_fragment:vL,lights_fragment_begin:yL,lights_fragment_maps:_L,lights_fragment_end:xL,lightprobes_pars_fragment:SL,logdepthbuf_fragment:bL,logdepthbuf_pars_fragment:ML,logdepthbuf_pars_vertex:EL,logdepthbuf_vertex:wL,map_fragment:TL,map_pars_fragment:CL,map_particle_fragment:IL,map_particle_pars_fragment:AL,metalnessmap_fragment:DL,metalnessmap_pars_fragment:RL,morphinstance_vertex:NL,morphcolor_vertex:PL,morphnormal_vertex:LL,morphtarget_pars_vertex:OL,morphtarget_vertex:FL,normal_fragment_begin:UL,normal_fragment_maps:kL,normal_pars_fragment:BL,normal_pars_vertex:VL,normal_vertex:HL,normalmap_pars_fragment:zL,clearcoat_normal_fragment_begin:GL,clearcoat_normal_fragment_maps:WL,clearcoat_pars_fragment:jL,iridescence_pars_fragment:$L,opaque_fragment:qL,packing:XL,premultiplied_alpha_fragment:YL,project_vertex:ZL,dithering_fragment:KL,dithering_pars_fragment:JL,roughnessmap_fragment:QL,roughnessmap_pars_fragment:eO,shadowmap_pars_fragment:tO,shadowmap_pars_vertex:nO,shadowmap_vertex:iO,shadowmask_pars_fragment:rO,skinbase_vertex:sO,skinning_pars_vertex:oO,skinning_vertex:aO,skinnormal_vertex:cO,specularmap_fragment:lO,specularmap_pars_fragment:uO,tonemapping_fragment:dO,tonemapping_pars_fragment:fO,transmission_fragment:hO,transmission_pars_fragment:pO,uv_pars_fragment:mO,uv_pars_vertex:gO,uv_vertex:vO,worldpos_vertex:yO,background_vert:_O,background_frag:xO,backgroundCube_vert:SO,backgroundCube_frag:bO,cube_vert:MO,cube_frag:EO,depth_vert:wO,depth_frag:TO,distance_vert:CO,distance_frag:IO,equirect_vert:AO,equirect_frag:DO,linedashed_vert:RO,linedashed_frag:NO,meshbasic_vert:PO,meshbasic_frag:LO,meshlambert_vert:OO,meshlambert_frag:FO,meshmatcap_vert:UO,meshmatcap_frag:kO,meshnormal_vert:BO,meshnormal_frag:VO,meshphong_vert:HO,meshphong_frag:zO,meshphysical_vert:GO,meshphysical_frag:WO,meshtoon_vert:jO,meshtoon_frag:$O,points_vert:qO,points_frag:XO,shadow_vert:YO,shadow_frag:ZO,sprite_vert:KO,sprite_frag:JO},fe={common:{diffuse:{value:new st(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Be}},envmap:{envMap:{value:null},envMapRotation:{value:new Be},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Be}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Be}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Be},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Be},normalScale:{value:new Pe(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Be},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Be}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Be}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Be}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new st(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},sunLights:{value:[],properties:{direction:{},color:{}}},sunLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},sunShadowMatrix:{value:[]},sunShadowCascade:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new F},probesMax:{value:new F},probesResolution:{value:new F}},points:{diffuse:{value:new st(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0},uvTransform:{value:new Be}},sprite:{diffuse:{value:new st(16777215)},opacity:{value:1},center:{value:new Pe(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}}},sr={basic:{uniforms:wn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.fog]),vertexShader:qe.meshbasic_vert,fragmentShader:qe.meshbasic_frag},lambert:{uniforms:wn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,fe.lights,{emissive:{value:new st(0)},envMapIntensity:{value:1}}]),vertexShader:qe.meshlambert_vert,fragmentShader:qe.meshlambert_frag},phong:{uniforms:wn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,fe.lights,{emissive:{value:new st(0)},specular:{value:new st(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:qe.meshphong_vert,fragmentShader:qe.meshphong_frag},standard:{uniforms:wn([fe.common,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.roughnessmap,fe.metalnessmap,fe.fog,fe.lights,{emissive:{value:new st(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:qe.meshphysical_vert,fragmentShader:qe.meshphysical_frag},toon:{uniforms:wn([fe.common,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.gradientmap,fe.fog,fe.lights,{emissive:{value:new st(0)}}]),vertexShader:qe.meshtoon_vert,fragmentShader:qe.meshtoon_frag},matcap:{uniforms:wn([fe.common,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,{matcap:{value:null}}]),vertexShader:qe.meshmatcap_vert,fragmentShader:qe.meshmatcap_frag},points:{uniforms:wn([fe.points,fe.fog]),vertexShader:qe.points_vert,fragmentShader:qe.points_frag},dashed:{uniforms:wn([fe.common,fe.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:qe.linedashed_vert,fragmentShader:qe.linedashed_frag},depth:{uniforms:wn([fe.common,fe.displacementmap]),vertexShader:qe.depth_vert,fragmentShader:qe.depth_frag},normal:{uniforms:wn([fe.common,fe.bumpmap,fe.normalmap,fe.displacementmap,{opacity:{value:1}}]),vertexShader:qe.meshnormal_vert,fragmentShader:qe.meshnormal_frag},sprite:{uniforms:wn([fe.sprite,fe.fog]),vertexShader:qe.sprite_vert,fragmentShader:qe.sprite_frag},background:{uniforms:{uvTransform:{value:new Be},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:qe.background_vert,fragmentShader:qe.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Be}},vertexShader:qe.backgroundCube_vert,fragmentShader:qe.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:qe.cube_vert,fragmentShader:qe.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:qe.equirect_vert,fragmentShader:qe.equirect_frag},distance:{uniforms:wn([fe.common,fe.displacementmap,{referencePosition:{value:new F},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:qe.distance_vert,fragmentShader:qe.distance_frag},shadow:{uniforms:wn([fe.lights,fe.fog,{color:{value:new st(0)},opacity:{value:1}}]),vertexShader:qe.shadow_vert,fragmentShader:qe.shadow_frag}};sr.physical={uniforms:wn([sr.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Be},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Be},clearcoatNormalScale:{value:new Pe(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Be},dispersion:{value:0},retroreflectivity:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Be},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Be},sheen:{value:0},sheenColor:{value:new st(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Be},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Be},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Be},transmissionSamplerSize:{value:new Pe},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Be},attenuationDistance:{value:0},attenuationColor:{value:new st(0)},specularColor:{value:new st(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Be},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Be},anisotropyVector:{value:new Pe},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Be}}]),vertexShader:qe.meshphysical_vert,fragmentShader:qe.meshphysical_frag};var Ep={r:0,b:0,g:0},QO=new Lt,Tw=new Be;Tw.set(-1,0,0,0,1,0,0,0,1);function eF(n,e,t,i,r,s){let o=new st(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(b){let A=b.isScene===!0?b.background:null;if(A&&A.isTexture){let S=b.backgroundBlurriness>0;A=e.get(A,S)}return A}function m(b){let A=!1,S=h(b);S===null?g(o,a):S&&S.isColor&&(g(S,1),A=!0);let M=n.xr.getEnvironmentBlendMode();M==="additive"?t.buffers.color.setClear(0,0,0,1,s):M==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||A)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(b,A){let S=h(A);S&&(S.isCubeTexture||S.mapping===Ol)?(l===void 0&&(l=new En(new rs(1,1,1),new Kn({name:"BackgroundCubeMaterial",uniforms:so(sr.backgroundCube.uniforms),vertexShader:sr.backgroundCube.vertexShader,fragmentShader:sr.backgroundCube.fragmentShader,side:Pn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(M,E,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=S,l.material.uniforms.backgroundBlurriness.value=A.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=A.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(QO.makeRotationFromEuler(A.backgroundRotation)).transpose(),S.isCubeTexture&&S.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(Tw),l.material.toneMapped=rt.getTransfer(S.colorSpace)!==_t,(u!==S||d!==S.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),l.layers.enableAll(),b.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new En(new no(2,2),new Kn({name:"BackgroundMaterial",uniforms:so(sr.background.uniforms),vertexShader:sr.background.vertexShader,fragmentShader:sr.background.fragmentShader,side:us,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=A.backgroundIntensity,c.material.toneMapped=rt.getTransfer(S.colorSpace)!==_t,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||d!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),c.layers.enableAll(),b.unshift(c,c.geometry,c.material,0,0,null))}function g(b,A){b.getRGB(Ep,r_(n)),t.buffers.color.setClear(Ep.r,Ep.g,Ep.b,A,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(b,A=1){o.set(b),a=A,g(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(b){a=b,g(o,a)},render:m,addToRenderList:y,dispose:p}}function tF(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(O,C,V,D,U){let $=!1,B=d(O,D,V,C);s!==B&&(s=B,l(s.object)),$=h(O,D,V,U),$&&m(O,D,V,U),U!==null&&e.update(U,n.ELEMENT_ARRAY_BUFFER),($||o)&&(o=!1,S(O,C,V,D),U!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(U).buffer))}function c(){return n.createVertexArray()}function l(O){return n.bindVertexArray(O)}function u(O){return n.deleteVertexArray(O)}function d(O,C,V,D){let U=D.wireframe===!0,$=i[C.id];$===void 0&&($={},i[C.id]=$);let B=O.isInstancedMesh===!0?O.id:0,X=$[B];X===void 0&&(X={},$[B]=X);let W=X[V.id];W===void 0&&(W={},X[V.id]=W);let Q=W[U];return Q===void 0&&(Q=f(c()),W[U]=Q),Q}function f(O){let C=[],V=[],D=[];for(let U=0;U<t;U++)C[U]=0,V[U]=0,D[U]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:C,enabledAttributes:V,attributeDivisors:D,object:O,attributes:{},index:null}}function h(O,C,V,D){let U=s.attributes,$=C.attributes,B=0,X=V.getAttributes();for(let W in X)if(X[W].location>=0){let ne=U[W],Ie=$[W];if(Ie===void 0&&(W==="instanceMatrix"&&O.instanceMatrix&&(Ie=O.instanceMatrix),W==="instanceColor"&&O.instanceColor&&(Ie=O.instanceColor)),ne===void 0||ne.attribute!==Ie||Ie&&ne.data!==Ie.data)return!0;B++}return s.attributesNum!==B||s.index!==D}function m(O,C,V,D){let U={},$=C.attributes,B=0,X=V.getAttributes();for(let W in X)if(X[W].location>=0){let ne=$[W];ne===void 0&&(W==="instanceMatrix"&&O.instanceMatrix&&(ne=O.instanceMatrix),W==="instanceColor"&&O.instanceColor&&(ne=O.instanceColor));let Ie={};Ie.attribute=ne,ne&&ne.data&&(Ie.data=ne.data),U[W]=Ie,B++}s.attributes=U,s.attributesNum=B,s.index=D}function y(){let O=s.newAttributes;for(let C=0,V=O.length;C<V;C++)O[C]=0}function g(O){p(O,0)}function p(O,C){let V=s.newAttributes,D=s.enabledAttributes,U=s.attributeDivisors;V[O]=1,D[O]===0&&(n.enableVertexAttribArray(O),D[O]=1),U[O]!==C&&(n.vertexAttribDivisor(O,C),U[O]=C)}function b(){let O=s.newAttributes,C=s.enabledAttributes;for(let V=0,D=C.length;V<D;V++)C[V]!==O[V]&&(n.disableVertexAttribArray(V),C[V]=0)}function A(O,C,V,D,U,$,B){B===!0?n.vertexAttribIPointer(O,C,V,U,$):n.vertexAttribPointer(O,C,V,D,U,$)}function S(O,C,V,D){y();let U=D.attributes,$=V.getAttributes(),B=C.defaultAttributeValues;for(let X in $){let W=$[X];if(W.location>=0){let Q=U[X];if(Q===void 0&&(X==="instanceMatrix"&&O.instanceMatrix&&(Q=O.instanceMatrix),X==="instanceColor"&&O.instanceColor&&(Q=O.instanceColor)),Q!==void 0){let ne=Q.normalized,Ie=Q.itemSize,Te=e.get(Q);if(Te===void 0)continue;let Ct=Te.buffer,ot=Te.type,dt=Te.bytesPerElement,Y=ot===n.INT||ot===n.UNSIGNED_INT||Q.gpuType===Vh;if(Q.isInterleavedBufferAttribute){let ee=Q.data,_e=ee.stride,Ve=Q.offset;if(ee.isInstancedInterleavedBuffer){for(let ve=0;ve<W.locationSize;ve++)p(W.location+ve,ee.meshPerAttribute);O.isInstancedMesh!==!0&&D._maxInstanceCount===void 0&&(D._maxInstanceCount=ee.meshPerAttribute*ee.count)}else for(let ve=0;ve<W.locationSize;ve++)g(W.location+ve);n.bindBuffer(n.ARRAY_BUFFER,Ct);for(let ve=0;ve<W.locationSize;ve++)A(W.location+ve,Ie/W.locationSize,ot,ne,_e*dt,(Ve+Ie/W.locationSize*ve)*dt,Y)}else{if(Q.isInstancedBufferAttribute){for(let ee=0;ee<W.locationSize;ee++)p(W.location+ee,Q.meshPerAttribute);O.isInstancedMesh!==!0&&D._maxInstanceCount===void 0&&(D._maxInstanceCount=Q.meshPerAttribute*Q.count)}else for(let ee=0;ee<W.locationSize;ee++)g(W.location+ee);n.bindBuffer(n.ARRAY_BUFFER,Ct);for(let ee=0;ee<W.locationSize;ee++)A(W.location+ee,Ie/W.locationSize,ot,ne,Ie*dt,Ie/W.locationSize*ee*dt,Y)}}else if(B!==void 0){let ne=B[X];if(ne!==void 0)switch(ne.length){case 2:n.vertexAttrib2fv(W.location,ne);break;case 3:n.vertexAttrib3fv(W.location,ne);break;case 4:n.vertexAttrib4fv(W.location,ne);break;default:n.vertexAttrib1fv(W.location,ne)}}}}b()}function M(){T();for(let O in i){let C=i[O];for(let V in C){let D=C[V];for(let U in D){let $=D[U];for(let B in $)u($[B].object),delete $[B];delete D[U]}}delete i[O]}}function E(O){if(i[O.id]===void 0)return;let C=i[O.id];for(let V in C){let D=C[V];for(let U in D){let $=D[U];for(let B in $)u($[B].object),delete $[B];delete D[U]}}delete i[O.id]}function I(O){for(let C in i){let V=i[C];for(let D in V){let U=V[D];if(U[O.id]===void 0)continue;let $=U[O.id];for(let B in $)u($[B].object),delete $[B];delete U[O.id]}}}function _(O){for(let C in i){let V=i[C],D=O.isInstancedMesh===!0?O.id:0,U=V[D];if(U!==void 0){for(let $ in U){let B=U[$];for(let X in B)u(B[X].object),delete B[X];delete U[$]}delete V[D],Object.keys(V).length===0&&delete i[C]}}}function T(){R(),o=!0,s!==r&&(s=r,l(s.object))}function R(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:T,resetDefaultState:R,dispose:M,releaseStatesOfGeometry:E,releaseStatesOfObject:_,releaseStatesOfProgram:I,initAttributes:y,enableAttribute:g,disableUnusedAttributes:b}}function nF(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let f=0;for(let h=0;h<u;h++)f+=l[h];t.update(f,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function iF(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(I){return!(I!==di&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let _=I===Ui&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==ei&&I!==Fi&&!_&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE))}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Re("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&f===!1&&Re("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),m=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),g=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),b=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),A=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),M=n.getParameter(n.MAX_SAMPLES),E=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:m,maxTextureSize:y,maxCubemapSize:g,maxAttributes:p,maxVertexUniforms:b,maxVaryings:A,maxFragmentUniforms:S,maxSamples:M,samples:E}}function rF(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Yn,a=new Be,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let m=d.clippingPlanes,y=d.clipIntersection,g=d.clipShadows,p=n.get(d);if(!r||m===null||m.length===0||s&&!g)s?u(null):l();else{let b=s?0:i,A=b*4,S=p.clippingState||null;c.value=S,S=u(m,f,A,h);for(let M=0;M!==A;++M)S[M]=t[M];p.clippingState=S,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=b}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,m){let y=d!==null?d.length:0,g=null;if(y!==0){if(g=c.value,m!==!0||g===null){let p=h+y*4,b=f.matrixWorldInverse;a.getNormalMatrix(b),(g===null||g.length<p)&&(g=new Float32Array(p));for(let A=0,S=h;A!==y;++A,S+=4)o.copy(d[A]).applyMatrix4(b,a),o.normal.toArray(g,S),g[S+3]=o.constant}c.value=g,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,g}}var La=4,sF=6,oF=20,aF=256,Gl=new Nl,rw=new st,c_=null,l_=0,u_=0,d_=!1,cF=new F,oo=new F,Tp=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=cF}=s;c_=this._renderer.getRenderTarget(),l_=this._renderer.getActiveCubeFace(),u_=this._renderer.getActiveMipmapLevel(),d_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=aw(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=ow(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(c_,l_,u_),this._renderer.xr.enabled=d_,e.scissorTest=!1,Pa(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===ds||e.mapping===ro?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),c_=this._renderer.getRenderTarget(),l_=this._renderer.getActiveCubeFace(),u_=this._renderer.getActiveMipmapLevel(),d_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:hn,minFilter:hn,generateMipmaps:!1,type:Ui,format:di,colorSpace:dl,depthBuffer:!1},r=sw(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=sw(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods}=lF(s)),this._blurMaterial=dF(s,e,t),this._ggxMaterial=uF(s,e,t)}return r}_compileMaterial(e){let t=new En(new pn,e);this._renderer.compile(t,Gl)}_sceneToCubeUV(e,t,i,r,s){let c=new Mn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(rw),d.toneMapping=Li,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new En(new rs,new Cr({name:"PMREM.Background",side:Pn,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,g=y.material,p=!1,b=e.background;b?b.isColor&&(g.color.copy(b),e.background=null,p=!0):(g.color.copy(rw),p=!0);for(let A=0;A<6;A++){let S=A%3;S===0?(c.up.set(0,l[A],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[A],s.y,s.z)):S===1?(c.up.set(0,0,l[A]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[A],s.z)):(c.up.set(0,l[A],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[A]));let M=this._cubeSize;Pa(r,S*M,A>2?M:0,M,M),d.setRenderTarget(r),p&&d.render(y,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=b}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===ds||e.mapping===ro;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=aw()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=ow());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Pa(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Gl)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=l*1.25,h=d*f,{_lodMax:m}=this,y=this._sizeLods[i],g=3*y*(i>m-La?i-m+La:0),p=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=m-t,Pa(s,g,p,3*y,2*y),r.setRenderTarget(s),r.render(a,Gl),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=m-i,Pa(e,g,p,3*y,2*y),r.setRenderTarget(e),r.render(a,Gl)}_blur(e,t,i,r){let s=this._pingPongRenderTarget,o=Math.min(r,Math.PI)/Math.SQRT2;this._blurPass(e,s,t,i,o),this._blurPass(s,e,i,i,o)}_blurPass(e,t,i,r,s){let o=this._renderer,a=this._blurMaterial,c=this._lodMeshes[r];c.material=a;let l=a.uniforms;l.envMap.value=e.texture,l.sigma.value=s,l.mipInt.value=this._lodMax-i;let u=this._sizeLods[r],d=3*u*(r>this._lodMax-La?r-this._lodMax+La:0),f=4*(this._cubeSize-u);Pa(t,d,f,3*u,2*u),o.setRenderTarget(t),o.render(c,Gl)}};function lF(n){let e=[],t=[],i=n,r=n-La+1+sF;for(let s=0;s<r;s++){let o=Math.pow(2,i);e.push(o);let a=1/(o-2),c=-a,l=1+a,u=[c,c,l,c,l,l,c,c,l,l,c,l],d=6,f=6,h=3,m=new Float32Array(h*f*d),y=new Float32Array(h*f*d);for(let p=0;p<d;p++){let b=p%3*2/3-1,A=p>2?0:-1,S=[b,A,0,b+2/3,A,0,b+2/3,A+1,0,b,A,0,b+2/3,A+1,0,b,A+1,0];m.set(S,h*f*p);for(let M=0;M<f;M++){let E=u[M*2]*2-1,I=u[M*2+1]*2-1;p===0?oo.set(1,I,E):p===1?oo.set(-E,1,-I):p===2?oo.set(-E,I,1):p===3?oo.set(-1,I,-E):p===4?oo.set(-E,-1,I):oo.set(E,I,-1),oo.toArray(y,(p*f+M)*h)}}let g=new pn;g.setAttribute("position",new fn(m,h)),g.setAttribute("outputDirection",new fn(y,h)),t.push(new En(g,null)),i>La&&i--}return{lodMeshes:t,sizeLods:e}}function sw(n,e,t){let i=new Un(n,e,t);return i.texture.mapping=Ol,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Pa(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function uF(n,e,t){return new Kn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:aF,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:Ap(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:nr,depthTest:!1,depthWrite:!1})}function dF(n,e,t){return new Kn({name:"SphericalGaussianBlur",defines:{SAMPLES:oF,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},sigma:{value:0},mipInt:{value:0}},vertexShader:Ap(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float sigma;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359
			#define GOLDEN_ANGLE 2.39996322973

			void main() {

				if ( sigma == 0.0 ) {

					gl_FragColor = vec4( bilinearCubeUV( envMap, vOutputDirection, mipInt ), 1.0 );
					return;

				}

				vec3 outputDirection = normalize( vOutputDirection );

				vec3 up = abs( outputDirection.z ) < 0.999 ? vec3( 0.0, 0.0, 1.0 ) : vec3( 1.0, 0.0, 0.0 );
				vec3 tangent = normalize( cross( up, outputDirection ) );
				vec3 bitangent = cross( outputDirection, tangent );

				// Truncate the kernel at three standard deviations or at the antipode.
				float thetaMax = min( 3.0 * sigma, PI );
				float truncation = 1.0 - exp( - 0.5 * thetaMax * thetaMax / ( sigma * sigma ) );

				vec3 accumColor = vec3( 0.0 );
				float accumWeight = 0.0;

				for ( int i = 0; i < SAMPLES; i ++ ) {

					// Stratified inverse-CDF sampling of the Gaussian, placed on a golden-angle spiral.
					float stratum = ( float( i ) + 0.5 ) / float( SAMPLES );
					float theta = sigma * sqrt( - 2.0 * log( 1.0 - stratum * truncation ) );
					float phi = float( i ) * GOLDEN_ANGLE;

					vec3 offset = cos( phi ) * tangent + sin( phi ) * bitangent;
					vec3 sampleDirection = cos( theta ) * outputDirection + sin( theta ) * offset;

					// Correct the planar sample density to solid angle.
					float weight = sin( theta ) / theta;

					accumColor += weight * bilinearCubeUV( envMap, sampleDirection, mipInt );
					accumWeight += weight;

				}

				gl_FragColor = vec4( accumColor / accumWeight, 1.0 );

			}
		`,blending:nr,depthTest:!1,depthWrite:!1})}function ow(){return new Kn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Ap(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:nr,depthTest:!1,depthWrite:!1})}function aw(){return new Kn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Ap(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:nr,depthTest:!1,depthWrite:!1})}function Ap(){return`

		precision mediump float;
		precision mediump int;

		attribute vec3 outputDirection;

		varying vec3 vOutputDirection;

		void main() {

			vOutputDirection = outputDirection;
			gl_Position = vec4( position, 1.0 );

		}
	`}var Cp=class extends Un{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new wl(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new rs(5,5,5),s=new Kn({name:"CubemapFromEquirect",uniforms:so(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Pn,blending:nr});s.uniforms.tEquirect.value=t;let o=new En(r,s),a=t.minFilter;return t.minFilter===fs&&(t.minFilter=hn),new Oh(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function fF(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?o(f):s(f)}function s(f){if(f&&f.isTexture){let h=f.mapping;if(h===Uh||h===kh)if(e.has(f)){let m=e.get(f).texture;return a(m,f.mapping)}else{let m=f.image;if(m&&m.height>0){let y=new Cp(m.height);return y.fromEquirectangularTexture(n,f),e.set(f,y),f.addEventListener("dispose",l),a(y.texture,f.mapping)}else return null}}return f}function o(f){if(f&&f.isTexture){let h=f.mapping,m=h===Uh||h===kh,y=h===ds||h===ro;if(m||y){let g=t.get(f),p=g!==void 0?g.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==p)return i===null&&(i=new Tp(n)),g=m?i.fromEquirectangular(f,g):i.fromCubemap(f,g),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),g.texture;if(g!==void 0)return g.texture;{let b=f.image;return m&&b&&b.height>0||y&&b&&c(b)?(i===null&&(i=new Tp(n)),g=m?i.fromEquirectangular(f):i.fromCubemap(f),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),f.addEventListener("dispose",u),g.texture):null}}}return f}function a(f,h){return h===Uh?f.mapping=ds:h===kh&&(f.mapping=ro),f}function c(f){let h=0,m=6;for(let y=0;y<m;y++)f[y]!==void 0&&h++;return h===m}function l(f){let h=f.target;h.removeEventListener("dispose",l);let m=e.get(h);m!==void 0&&(e.delete(h),m.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let m=t.get(h);m!==void 0&&(t.delete(h),m.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function hF(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Js("WebGLRenderer: "+i+" extension not supported."),r}}}function pF(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let m in f.attributes)e.remove(f.attributes[m]);f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,m=d.attributes.position,y=0;if(m===void 0)return;if(h!==null){let b=h.array;y=h.version;for(let A=0,S=b.length;A<S;A+=3){let M=b[A+0],E=b[A+1],I=b[A+2];f.push(M,E,E,I,I,M)}}else{let b=m.array;y=m.version;for(let A=0,S=b.length/3-1;A<S;A+=3){let M=A+0,E=A+1,I=A+2;f.push(M,E,E,I,I,M)}}let g=new(m.count>=65535?Sl:xl)(f,1);g.version=y;let p=s.get(d);p&&e.remove(p),s.set(d,g)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function mF(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,f){n.drawElements(i,f,s,d*o),t.update(f,i,1)}function l(d,f,h){h!==0&&(n.drawElementsInstanced(i,f,s,d*o,h),t.update(f,i,h))}function u(d,f,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,d,0,h);let y=0;for(let g=0;g<h;g++)y+=f[g];t.update(y,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function gF(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Le("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function vF(n,e,t){let i=new WeakMap,r=new Ot;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let R=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",R)};var h=R;f!==void 0&&f.texture.dispose();let m=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,g=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],b=a.morphAttributes.normal||[],A=a.morphAttributes.color||[],S=0;m===!0&&(S=1),y===!0&&(S=2),g===!0&&(S=3);let M=a.attributes.position.count*S,E=1;M>e.maxTextureSize&&(E=Math.ceil(M/e.maxTextureSize),M=e.maxTextureSize);let I=new Float32Array(M*E*4*d),_=new gl(I,M,E,d);_.type=Fi,_.needsUpdate=!0;let T=S*4;for(let O=0;O<d;O++){let C=p[O],V=b[O],D=A[O],U=M*E*4*O;for(let $=0;$<C.count;$++){let B=$*T;m===!0&&(r.fromBufferAttribute(C,$),I[U+B+0]=r.x,I[U+B+1]=r.y,I[U+B+2]=r.z,I[U+B+3]=0),y===!0&&(r.fromBufferAttribute(V,$),I[U+B+4]=r.x,I[U+B+5]=r.y,I[U+B+6]=r.z,I[U+B+7]=0),g===!0&&(r.fromBufferAttribute(D,$),I[U+B+8]=r.x,I[U+B+9]=r.y,I[U+B+10]=r.z,I[U+B+11]=D.itemSize===4?r.w:1)}}f={count:d,texture:_,size:new Pe(M,E)},i.set(a,f),a.addEventListener("dispose",R)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let m=0;for(let g=0;g<l.length;g++)m+=l[g];let y=a.morphTargetsRelative?1:1-m;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function yF(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(s.get(f)!==u&&(e.update(f),s.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return f}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var _F={[By]:"LINEAR_TONE_MAPPING",[Vy]:"REINHARD_TONE_MAPPING",[Hy]:"CINEON_TONE_MAPPING",[zy]:"ACES_FILMIC_TONE_MAPPING",[Wy]:"AGX_TONE_MAPPING",[jy]:"NEUTRAL_TONE_MAPPING",[Gy]:"CUSTOM_TONE_MAPPING"};function xF(n,e,t,i,r,s){let o=new Un(e,t,{type:n,depthBuffer:r,stencilBuffer:s,samples:i?4:0,storeMultisampledDepthBuffer:!1,storeMultisampledStencilBuffer:!1,resolveDepthBuffer:!1,resolveStencilBuffer:!1}),a=null,c=null,l=new pn;l.setAttribute("position",new jt([-1,3,0,-1,-1,0,3,-1,0],3)),l.setAttribute("uv",new jt([0,2,0,0,2,0],2));let u=new Eh({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),d=new En(l,u),f=new Nl(-1,1,1,-1,0,1),h=null,m=null,y=!1,g,p=null,b=[],A=!1;this.setSize=function(S,M){o.setSize(S,M),a!==null&&a.setSize(S,M),c!==null&&c.setSize(S,M);for(let E=0;E<b.length;E++){let I=b[E];I.setSize&&I.setSize(S,M)}},this.setEffects=function(S){b=S,A=b.length>0&&b[0].isRenderPass===!0;let M=o.width,E=o.height;b.length>0&&a===null&&(a=new Un(M,E,{type:Ui,depthBuffer:!1,stencilBuffer:!1}),c=new Un(M,E,{type:Ui,depthBuffer:!1,stencilBuffer:!1}));for(let I=0;I<b.length;I++){let _=b[I];_.setSize&&_.setSize(M,E)}},this.begin=function(S,M){if(y||S.toneMapping===Li&&b.length===0)return!1;if(p=M,M!==null){let E=M.width,I=M.height;(o.width!==E||o.height!==I)&&this.setSize(E,I)}return A===!1&&S.setRenderTarget(o),g=S.toneMapping,S.toneMapping=Li,!0},this.hasRenderPass=function(){return A},this.end=function(S,M){S.toneMapping=g,y=!0;let E=o,I=a;for(let _=0;_<b.length;_++){let T=b[_];T.enabled!==!1&&(T.render(S,I,E,M),T.needsSwap!==!1&&(E=I,I=I===a?c:a))}if(h!==S.outputColorSpace||m!==S.toneMapping){h=S.outputColorSpace,m=S.toneMapping,u.defines={},rt.getTransfer(h)===_t&&(u.defines.SRGB_TRANSFER="");let _=_F[m];_&&(u.defines[_]=""),u.needsUpdate=!0}u.uniforms.tDiffuse.value=E.texture,S.setRenderTarget(p),S.render(d,f),p=null,y=!1},this.isCompositing=function(){return y},this.dispose=function(){o.dispose(),a!==null&&a.dispose(),c!==null&&c.dispose(),l.dispose(),u.dispose()}}var Cw=new ir,p_=new is(1,1),Iw=new gl,Aw=new vh,Dw=new wl,cw=[],lw=[],uw=new Float32Array(16),dw=new Float32Array(9),fw=new Float32Array(4);function Fa(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=cw[r];if(s===void 0&&(s=new Float32Array(r),cw[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Kt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Jt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Dp(n,e){let t=lw[e];t===void 0&&(t=new Int32Array(e),lw[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function SF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function bF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2fv(this.addr,e),Jt(t,e)}}function MF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Kt(t,e))return;n.uniform3fv(this.addr,e),Jt(t,e)}}function EF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4fv(this.addr,e),Jt(t,e)}}function wF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;fw.set(i),n.uniformMatrix2fv(this.addr,!1,fw),Jt(t,i)}}function TF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;dw.set(i),n.uniformMatrix3fv(this.addr,!1,dw),Jt(t,i)}}function CF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;uw.set(i),n.uniformMatrix4fv(this.addr,!1,uw),Jt(t,i)}}function IF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function AF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2iv(this.addr,e),Jt(t,e)}}function DF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3iv(this.addr,e),Jt(t,e)}}function RF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4iv(this.addr,e),Jt(t,e)}}function NF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function PF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2uiv(this.addr,e),Jt(t,e)}}function LF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3uiv(this.addr,e),Jt(t,e)}}function OF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4uiv(this.addr,e),Jt(t,e)}}function FF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(p_.compareFunction=t.isReversedDepthBuffer()?Mp:bp,s=p_):s=Cw,t.setTexture2D(e||s,r)}function UF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||Aw,r)}function kF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||Dw,r)}function BF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||Iw,r)}function VF(n){switch(n){case 5126:return SF;case 35664:return bF;case 35665:return MF;case 35666:return EF;case 35674:return wF;case 35675:return TF;case 35676:return CF;case 5124:case 35670:return IF;case 35667:case 35671:return AF;case 35668:case 35672:return DF;case 35669:case 35673:return RF;case 5125:return NF;case 36294:return PF;case 36295:return LF;case 36296:return OF;case 35678:case 36198:case 36298:case 36306:case 35682:return FF;case 35679:case 36299:case 36307:return UF;case 35680:case 36300:case 36308:case 36293:return kF;case 36289:case 36303:case 36311:case 36292:return BF}}function HF(n,e){n.uniform1fv(this.addr,e)}function zF(n,e){let t=Fa(e,this.size,2);n.uniform2fv(this.addr,t)}function GF(n,e){let t=Fa(e,this.size,3);n.uniform3fv(this.addr,t)}function WF(n,e){let t=Fa(e,this.size,4);n.uniform4fv(this.addr,t)}function jF(n,e){let t=Fa(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function $F(n,e){let t=Fa(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function qF(n,e){let t=Fa(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function XF(n,e){n.uniform1iv(this.addr,e)}function YF(n,e){n.uniform2iv(this.addr,e)}function ZF(n,e){n.uniform3iv(this.addr,e)}function KF(n,e){n.uniform4iv(this.addr,e)}function JF(n,e){n.uniform1uiv(this.addr,e)}function QF(n,e){n.uniform2uiv(this.addr,e)}function eU(n,e){n.uniform3uiv(this.addr,e)}function tU(n,e){n.uniform4uiv(this.addr,e)}function nU(n,e,t){let i=this.cache,r=e.length,s=Dp(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=p_:o=Cw;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function iU(n,e,t){let i=this.cache,r=e.length,s=Dp(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||Aw,s[o])}function rU(n,e,t){let i=this.cache,r=e.length,s=Dp(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||Dw,s[o])}function sU(n,e,t){let i=this.cache,r=e.length,s=Dp(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||Iw,s[o])}function oU(n){switch(n){case 5126:return HF;case 35664:return zF;case 35665:return GF;case 35666:return WF;case 35674:return jF;case 35675:return $F;case 35676:return qF;case 5124:case 35670:return XF;case 35667:case 35671:return YF;case 35668:case 35672:return ZF;case 35669:case 35673:return KF;case 5125:return JF;case 36294:return QF;case 36295:return eU;case 36296:return tU;case 35678:case 36198:case 36298:case 36306:case 35682:return nU;case 35679:case 36299:case 36307:return iU;case 35680:case 36300:case 36308:case 36293:return rU;case 36289:case 36303:case 36311:case 36292:return sU}}var m_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=VF(t.type)}},g_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=oU(t.type)}},v_=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},f_=/(\w+)(\])?(\[|\.)?/g;function hw(n,e){n.seq.push(e),n.map[e.id]=e}function aU(n,e,t){let i=n.name,r=i.length;for(f_.lastIndex=0;;){let s=f_.exec(i),o=f_.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){hw(t,l===void 0?new m_(a,n,e):new g_(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new v_(a),hw(t,d)),t=d}}}var Oa=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);aU(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function pw(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var cU=37297,lU=0;function uU(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var mw=new Be;function dU(n){rt._getMatrix(mw,rt.workingColorSpace,n);let e=`mat3( ${mw.elements.map(t=>t.toFixed(4))} )`;switch(rt.getTransfer(n)){case fl:return[e,"LinearTransferOETF"];case _t:return[e,"sRGBTransferOETF"];default:return Re("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function gw(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+uU(n.getShaderSource(e),a)}else return s}function fU(n,e){let t=dU(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var hU={[By]:"Linear",[Vy]:"Reinhard",[Hy]:"Cineon",[zy]:"ACESFilmic",[Wy]:"AgX",[jy]:"Neutral",[Gy]:"Custom"};function pU(n,e){let t=hU[e];return t===void 0?(Re("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var wp=new F;function mU(){rt.getLuminanceCoefficients(wp);let n=wp.x.toFixed(4),e=wp.y.toFixed(4),t=wp.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function gU(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(jl).join(`
`)}function vU(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function yU(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function jl(n){return n!==""}function vw(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_SUN_LIGHTS/g,e.numSunLights).replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_SUN_LIGHT_SHADOWS/g,e.numSunLightShadows).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function yw(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var _U=/^[ \t]*#include +<([\w\d./]+)>/gm;function y_(n){return n.replace(_U,SU)}var xU=new Map;function SU(n,e){let t=qe[e];if(t===void 0){let i=xU.get(e);if(i!==void 0)t=qe[i],Re('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("THREE.WebGLProgram: Can not resolve #include <"+e+">")}return y_(t)}var bU=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function _w(n){return n.replace(bU,MU)}function MU(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function xw(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var EU={[Ll]:"SHADOWMAP_TYPE_PCF",[Aa]:"SHADOWMAP_TYPE_VSM"};function wU(n){return EU[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var TU={[ds]:"ENVMAP_TYPE_CUBE",[ro]:"ENVMAP_TYPE_CUBE",[Ol]:"ENVMAP_TYPE_CUBE_UV"};function CU(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":TU[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var IU={[ro]:"ENVMAP_MODE_REFRACTION"};function AU(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":IU[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var DU={[ky]:"ENVMAP_BLENDING_MULTIPLY",[kE]:"ENVMAP_BLENDING_MIX",[BE]:"ENVMAP_BLENDING_ADD"};function RU(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":DU[n.combine]||"ENVMAP_BLENDING_NONE"}function NU(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function PU(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=wU(t),l=CU(t),u=AU(t),d=RU(t),f=NU(t),h=gU(t),m=vU(s),y=r.createProgram(),g,p,b=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(g=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(jl).join(`
`),g.length>0&&(g+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(jl).join(`
`),p.length>0&&(p+=`
`)):(g=[xw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(jl).join(`
`),p=[xw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.retroreflection?"#define USE_RETROREFLECTION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Li?"#define TONE_MAPPING":"",t.toneMapping!==Li?qe.tonemapping_pars_fragment:"",t.toneMapping!==Li?pU("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",qe.colorspace_pars_fragment,fU("linearToOutputTexel",t.outputColorSpace),mU(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(jl).join(`
`)),o=y_(o),o=vw(o,t),o=yw(o,t),a=y_(a),a=vw(a,t),a=yw(a,t),o=_w(o),a=_w(a),t.isRawShaderMaterial!==!0&&(b=`#version 300 es
`,g=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+g,p=["#define varying in",t.glslVersion===t_?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===t_?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let A=b+g+o,S=b+p+a,M=pw(r,r.VERTEX_SHADER,A),E=pw(r,r.FRAGMENT_SHADER,S);r.attachShader(y,M),r.attachShader(y,E),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.hasPositionAttribute===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function I(O){if(n.debug.checkShaderErrors){let C=r.getProgramInfoLog(y)||"",V=r.getShaderInfoLog(M)||"",D=r.getShaderInfoLog(E)||"",U=C.trim(),$=V.trim(),B=D.trim(),X=!0,W=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(X=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,M,E);else{let Q=gw(r,M,"vertex"),ne=gw(r,E,"fragment");Le("WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+O.name+`
Material Type: `+O.type+`

Program Info Log: `+U+`
`+Q+`
`+ne)}else U!==""?Re("WebGLProgram: Program Info Log:",U):($===""||B==="")&&(W=!1);W&&(O.diagnostics={runnable:X,programLog:U,vertexShader:{log:$,prefix:g},fragmentShader:{log:B,prefix:p}})}r.deleteShader(M),r.deleteShader(E),_=new Oa(r,y),T=yU(r,y)}let _;this.getUniforms=function(){return _===void 0&&I(this),_};let T;this.getAttributes=function(){return T===void 0&&I(this),T};let R=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return R===!1&&(R=r.getProgramParameter(y,cU)),R},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=lU++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=M,this.fragmentShader=E,this}var LU=0,__=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e,t,i){let r=this._getShaderCacheForMaterial(e);return r.has(t)===!1&&(r.add(t),t.usedTimes++),r.has(i)===!1&&(r.add(i),i.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderStage(e){return this._getShaderStage(e.vertexShader)}getFragmentShaderStage(e){return this._getShaderStage(e.fragmentShader)}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new x_(e),t.set(e,i)),i}},x_=class{constructor(e){this.id=LU++,this.code=e,this.usedTimes=0}};function OU(n){return n===ps||n===Hl||n===zl}function FU(n,e,t,i,r,s){let o=new yl,a=new __,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function m(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,T,R,O,C,V){let D=O.fog,U=C.geometry,$=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?O.environment:null,B=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,X=e.get(_.envMap||$,B),W=X&&X.mapping===Ol?X.image.height:null,Q=h[_.type];_.precision!==null&&(f=i.getMaxPrecision(_.precision),f!==_.precision&&Re("WebGLProgram.getParameters:",_.precision,"not supported, using",f,"instead."));let ne=U.morphAttributes.position||U.morphAttributes.normal||U.morphAttributes.color,Ie=ne!==void 0?ne.length:0,Te=0;U.morphAttributes.position!==void 0&&(Te=1),U.morphAttributes.normal!==void 0&&(Te=2),U.morphAttributes.color!==void 0&&(Te=3);let Ct,ot,dt,Y;if(Q){let At=sr[Q];Ct=At.vertexShader,ot=At.fragmentShader}else{Ct=_.vertexShader,ot=_.fragmentShader;let At=a.getVertexShaderStage(_),mt=a.getFragmentShaderStage(_);a.update(_,At,mt),dt=At.id,Y=mt.id}let ee=n.getRenderTarget(),_e=n.state.buffers.depth.getReversed(),Ve=C.isInstancedMesh===!0,ve=C.isBatchedMesh===!0,Ye=!!_.map,$t=!!_.matcap,Je=!!X,ut=!!_.aoMap,It=!!_.lightMap,et=!!_.bumpMap&&_.wireframe===!1,Nt=!!_.normalMap,en=!!_.displacementMap,Ln=!!_.emissiveMap,Pt=!!_.metalnessMap,Ht=!!_.roughnessMap,L=_.anisotropy>0,mn=_.clearcoat>0,xt=_.dispersion>0,w=_.retroreflectivity>0,v=_.iridescence>0,k=_.sheen>0,G=_.transmission>0,q=L&&!!_.anisotropyMap,ie=mn&&!!_.clearcoatMap,se=mn&&!!_.clearcoatNormalMap,Z=mn&&!!_.clearcoatRoughnessMap,J=v&&!!_.iridescenceMap,ae=v&&!!_.iridescenceThicknessMap,Ee=k&&!!_.sheenColorMap,de=k&&!!_.sheenRoughnessMap,ce=!!_.specularMap,we=!!_.specularColorMap,Ne=!!_.specularIntensityMap,We=G&&!!_.transmissionMap,P=G&&!!_.thicknessMap,le=!!_.gradientMap,K=!!_.alphaMap,ue=_.alphaTest>0,me=!!_.alphaHash,te=!!_.extensions,Ce=Li;_.toneMapped&&(ee===null||ee.isXRRenderTarget===!0)&&(Ce=n.toneMapping);let be={shaderID:Q,shaderType:_.type,shaderName:_.name,vertexShader:Ct,fragmentShader:ot,defines:_.defines,customVertexShaderID:dt,customFragmentShaderID:Y,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:f,batching:ve,batchingColor:ve&&C._colorsTexture!==null,instancing:Ve,instancingColor:Ve&&C.instanceColor!==null,instancingMorph:Ve&&C.morphTexture!==null,outputColorSpace:ee===null?n.outputColorSpace:ee.isXRRenderTarget===!0?ee.texture.colorSpace:rt.workingColorSpace,alphaToCoverage:!!_.alphaToCoverage,map:Ye,matcap:$t,envMap:Je,envMapMode:Je&&X.mapping,envMapCubeUVHeight:W,aoMap:ut,lightMap:It,bumpMap:et,normalMap:Nt,displacementMap:en,emissiveMap:Ln,normalMapObjectSpace:Nt&&_.normalMapType===zE,normalMapTangentSpace:Nt&&_.normalMapType===Qy,packedNormalMap:Nt&&_.normalMapType===Qy&&OU(_.normalMap.format),metalnessMap:Pt,roughnessMap:Ht,anisotropy:L,anisotropyMap:q,clearcoat:mn,clearcoatMap:ie,clearcoatNormalMap:se,clearcoatRoughnessMap:Z,dispersion:xt,retroreflection:w,iridescence:v,iridescenceMap:J,iridescenceThicknessMap:ae,sheen:k,sheenColorMap:Ee,sheenRoughnessMap:de,specularMap:ce,specularColorMap:we,specularIntensityMap:Ne,transmission:G,transmissionMap:We,thicknessMap:P,gradientMap:le,opaque:_.transparent===!1&&_.blending===Da&&_.alphaToCoverage===!1,alphaMap:K,alphaTest:ue,alphaHash:me,combine:_.combine,mapUv:Ye&&m(_.map.channel),aoMapUv:ut&&m(_.aoMap.channel),lightMapUv:It&&m(_.lightMap.channel),bumpMapUv:et&&m(_.bumpMap.channel),normalMapUv:Nt&&m(_.normalMap.channel),displacementMapUv:en&&m(_.displacementMap.channel),emissiveMapUv:Ln&&m(_.emissiveMap.channel),metalnessMapUv:Pt&&m(_.metalnessMap.channel),roughnessMapUv:Ht&&m(_.roughnessMap.channel),anisotropyMapUv:q&&m(_.anisotropyMap.channel),clearcoatMapUv:ie&&m(_.clearcoatMap.channel),clearcoatNormalMapUv:se&&m(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:Z&&m(_.clearcoatRoughnessMap.channel),iridescenceMapUv:J&&m(_.iridescenceMap.channel),iridescenceThicknessMapUv:ae&&m(_.iridescenceThicknessMap.channel),sheenColorMapUv:Ee&&m(_.sheenColorMap.channel),sheenRoughnessMapUv:de&&m(_.sheenRoughnessMap.channel),specularMapUv:ce&&m(_.specularMap.channel),specularColorMapUv:we&&m(_.specularColorMap.channel),specularIntensityMapUv:Ne&&m(_.specularIntensityMap.channel),transmissionMapUv:We&&m(_.transmissionMap.channel),thicknessMapUv:P&&m(_.thicknessMap.channel),alphaMapUv:K&&m(_.alphaMap.channel),vertexTangents:!!U.attributes.tangent&&(Nt||L),vertexNormals:!!U.attributes.normal,vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!U.attributes.color&&U.attributes.color.itemSize===4,pointsUvs:C.isPoints===!0&&!!U.attributes.uv&&(Ye||K),fog:!!D,useFog:_.fog===!0,fogExp2:!!D&&D.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||U.attributes.normal===void 0&&Nt===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:_e,skinning:C.isSkinnedMesh===!0,hasPositionAttribute:U.attributes.position!==void 0,morphTargets:U.morphAttributes.position!==void 0,morphNormals:U.morphAttributes.normal!==void 0,morphColors:U.morphAttributes.color!==void 0,morphTargetsCount:Ie,morphTextureStride:Te,numSunLights:T.sun.length,numDirLights:T.directional.length,numPointLights:T.point.length,numSpotLights:T.spot.length,numSpotLightMaps:T.spotLightMap.length,numRectAreaLights:T.rectArea.length,numHemiLights:T.hemi.length,numSunLightShadows:T.sunShadowMap.length,numDirLightShadows:T.directionalShadowMap.length,numPointLightShadows:T.pointShadowMap.length,numSpotLightShadows:T.spotShadowMap.length,numSpotLightShadowsWithMaps:T.numSpotLightShadowsWithMaps,numLightProbes:T.numLightProbes,numLightProbeGrids:V.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&R.length>0,shadowMapType:n.shadowMap.type,toneMapping:Ce,decodeVideoTexture:Ye&&_.map.isVideoTexture===!0&&rt.getTransfer(_.map.colorSpace)===_t,decodeVideoTextureEmissive:Ln&&_.emissiveMap.isVideoTexture===!0&&rt.getTransfer(_.emissiveMap.colorSpace)===_t,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===Qn,flipSided:_.side===Pn,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:te&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(te&&_.extensions.multiDraw===!0||ve)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return be.vertexUv1s=c.has(1),be.vertexUv2s=c.has(2),be.vertexUv3s=c.has(3),c.clear(),be}function g(_){let T=[];if(_.shaderID?T.push(_.shaderID):(T.push(_.customVertexShaderID),T.push(_.customFragmentShaderID)),_.defines!==void 0)for(let R in _.defines)T.push(R),T.push(_.defines[R]);return _.isRawShaderMaterial===!1&&(p(T,_),b(T,_),T.push(n.outputColorSpace)),T.push(_.customProgramCacheKey),T.join()}function p(_,T){_.push(T.precision),_.push(T.outputColorSpace),_.push(T.envMapMode),_.push(T.envMapCubeUVHeight),_.push(T.mapUv),_.push(T.alphaMapUv),_.push(T.lightMapUv),_.push(T.aoMapUv),_.push(T.bumpMapUv),_.push(T.normalMapUv),_.push(T.displacementMapUv),_.push(T.emissiveMapUv),_.push(T.metalnessMapUv),_.push(T.roughnessMapUv),_.push(T.anisotropyMapUv),_.push(T.clearcoatMapUv),_.push(T.clearcoatNormalMapUv),_.push(T.clearcoatRoughnessMapUv),_.push(T.iridescenceMapUv),_.push(T.iridescenceThicknessMapUv),_.push(T.sheenColorMapUv),_.push(T.sheenRoughnessMapUv),_.push(T.specularMapUv),_.push(T.specularColorMapUv),_.push(T.specularIntensityMapUv),_.push(T.transmissionMapUv),_.push(T.thicknessMapUv),_.push(T.combine),_.push(T.fogExp2),_.push(T.sizeAttenuation),_.push(T.morphTargetsCount),_.push(T.morphAttributeCount),_.push(T.numSunLights),_.push(T.numDirLights),_.push(T.numPointLights),_.push(T.numSpotLights),_.push(T.numSpotLightMaps),_.push(T.numHemiLights),_.push(T.numRectAreaLights),_.push(T.numSunLightShadows),_.push(T.numDirLightShadows),_.push(T.numPointLightShadows),_.push(T.numSpotLightShadows),_.push(T.numSpotLightShadowsWithMaps),_.push(T.numLightProbes),_.push(T.shadowMapType),_.push(T.toneMapping),_.push(T.numClippingPlanes),_.push(T.numClipIntersection),_.push(T.depthPacking)}function b(_,T){o.disableAll(),T.instancing&&o.enable(0),T.instancingColor&&o.enable(1),T.instancingMorph&&o.enable(2),T.matcap&&o.enable(3),T.envMap&&o.enable(4),T.normalMapObjectSpace&&o.enable(5),T.normalMapTangentSpace&&o.enable(6),T.clearcoat&&o.enable(7),T.iridescence&&o.enable(8),T.alphaTest&&o.enable(9),T.vertexColors&&o.enable(10),T.vertexAlphas&&o.enable(11),T.vertexUv1s&&o.enable(12),T.vertexUv2s&&o.enable(13),T.vertexUv3s&&o.enable(14),T.vertexTangents&&o.enable(15),T.anisotropy&&o.enable(16),T.alphaHash&&o.enable(17),T.batching&&o.enable(18),T.dispersion&&o.enable(19),T.retroreflection&&o.enable(24),T.batchingColor&&o.enable(20),T.gradientMap&&o.enable(21),T.packedNormalMap&&o.enable(22),T.vertexNormals&&o.enable(23),_.push(o.mask),o.disableAll(),T.fog&&o.enable(0),T.useFog&&o.enable(1),T.flatShading&&o.enable(2),T.logarithmicDepthBuffer&&o.enable(3),T.reversedDepthBuffer&&o.enable(4),T.skinning&&o.enable(5),T.morphTargets&&o.enable(6),T.morphNormals&&o.enable(7),T.morphColors&&o.enable(8),T.premultipliedAlpha&&o.enable(9),T.shadowMapEnabled&&o.enable(10),T.doubleSided&&o.enable(11),T.flipSided&&o.enable(12),T.useDepthPacking&&o.enable(13),T.dithering&&o.enable(14),T.transmission&&o.enable(15),T.sheen&&o.enable(16),T.opaque&&o.enable(17),T.pointsUvs&&o.enable(18),T.decodeVideoTexture&&o.enable(19),T.decodeVideoTextureEmissive&&o.enable(20),T.alphaToCoverage&&o.enable(21),T.numLightProbeGrids>0&&o.enable(22),T.hasPositionAttribute&&o.enable(23),_.push(o.mask)}function A(_){let T=h[_.type],R;if(T){let O=sr[T];R=nw.clone(O.uniforms)}else R=_.uniforms;return R}function S(_,T){let R=u.get(T);return R!==void 0?++R.usedTimes:(R=new PU(n,T,_,r),l.push(R),u.set(T,R)),R}function M(_){if(--_.usedTimes===0){let T=l.indexOf(_);l[T]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function E(_){a.remove(_)}function I(){a.dispose()}return{getParameters:y,getProgramCacheKey:g,getUniforms:A,acquireProgram:S,releaseProgram:M,releaseShaderCache:E,programs:l,dispose:I}}function UU(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function kU(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function Sw(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function bw(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,m,y,g,p){let b=n[e];return b===void 0?(b={id:f.id,object:f,geometry:h,material:m,materialVariant:o(f),groupOrder:y,renderOrder:f.renderOrder,z:g,group:p},n[e]=b):(b.id=f.id,b.object=f,b.geometry=h,b.material=m,b.materialVariant=o(f),b.groupOrder=y,b.renderOrder=f.renderOrder,b.z=g,b.group=p),e++,b}function c(f,h,m,y,g,p,b){b.reversedDepth===!0&&(g=-g);let A=a(f,h,m,y,g,p);m.transmission>0?i.push(A):m.transparent===!0?r.push(A):t.push(A)}function l(f,h,m,y,g,p){let b=a(f,h,m,y,g,p);m.transmission>0?i.unshift(b):m.transparent===!0?r.unshift(b):t.unshift(b)}function u(f,h){t.length>1&&t.sort(f||kU),i.length>1&&i.sort(h||Sw),r.length>1&&r.sort(h||Sw)}function d(){for(let f=e,h=n.length;f<h;f++){let m=n[f];if(m.id===null)break;m.id=null,m.object=null,m.geometry=null,m.material=null,m.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function BU(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new bw,n.set(i,[o])):r>=s.length?(o=new bw,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function VU(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"SunLight":case"DirectionalLight":t={direction:new F,color:new st};break;case"SpotLight":t={position:new F,direction:new F,color:new st,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new F,color:new st,distance:0,decay:0};break;case"HemisphereLight":t={direction:new F,skyColor:new st,groundColor:new st};break;case"RectAreaLight":t={color:new st,position:new F,halfWidth:new F,halfHeight:new F};break}return n[e.id]=t,t}}}function HU(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"SunLight":case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Pe};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Pe};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Pe,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var zU=0;function GU(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function WU(n){let e=new VU,t=HU(),i={version:0,hash:{sunLength:-1,directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numSunShadows:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],sun:[],sunShadow:[],sunShadowMap:[],sunShadowMatrix:[],sunShadowCascade:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new F);let r=new F,s=new Lt,o=new Lt;function a(l){let u=0,d=0,f=0;for(let C=0;C<9;C++)i.probe[C].set(0,0,0);let h=0,m=0,y=0,g=0,p=0,b=0,A=0,S=0,M=0,E=0,I=0,_=0,T=0,R=0;l.sort(GU);for(let C=0,V=l.length;C<V;C++){let D=l[C],U=D.color,$=D.intensity,B=D.distance,X=null;if(D.shadow&&D.shadow.map&&(D.shadow.map.texture.format===ps?X=D.shadow.map.texture:X=D.shadow.map.depthTexture||D.shadow.map.texture),D.isAmbientLight)u+=U.r*$,d+=U.g*$,f+=U.b*$;else if(D.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(D.sh.coefficients[W],$);R++}else if(D.isSunLight){let W=e.get(D);if(W.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let Q=D.shadow,ne=t.get(D);ne.shadowIntensity=Q.intensity,ne.shadowBias=Q.bias,ne.shadowNormalBias=Q.normalBias,ne.shadowRadius=Q.radius,ne.shadowMapSize.copy(Q.mapSize).multiply(Q.getFrameExtents()),i.sunShadow[m]=ne,i.sunShadowMap[m]=X;let Ie=Q.getViewportCount();for(let Te=0;Te<Ie;Te++)i.sunShadowMatrix[y+Te]=Q.getMatrix(Te),i.sunShadowCascade[y+Te]=Q._cascadeData[Te];y+=Ie,m++}i.sun[h]=W,h++}else if(D.isDirectionalLight){let W=e.get(D);if(W.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let Q=D.shadow,ne=t.get(D);ne.shadowIntensity=Q.intensity,ne.shadowBias=Q.bias,ne.shadowNormalBias=Q.normalBias,ne.shadowRadius=Q.radius,ne.shadowMapSize=Q.mapSize,i.directionalShadow[g]=ne,i.directionalShadowMap[g]=X,i.directionalShadowMatrix[g]=D.shadow.matrix,M++}i.directional[g]=W,g++}else if(D.isSpotLight){let W=e.get(D);W.position.setFromMatrixPosition(D.matrixWorld),W.color.copy(U).multiplyScalar($),W.distance=B,W.coneCos=Math.cos(D.angle),W.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),W.decay=D.decay,i.spot[b]=W;let Q=D.shadow;if(D.map&&(i.spotLightMap[_]=D.map,_++,Q.updateMatrices(D),D.castShadow&&T++),i.spotLightMatrix[b]=Q.matrix,D.castShadow){let ne=t.get(D);ne.shadowIntensity=Q.intensity,ne.shadowBias=Q.bias,ne.shadowNormalBias=Q.normalBias,ne.shadowRadius=Q.radius,ne.shadowMapSize=Q.mapSize,i.spotShadow[b]=ne,i.spotShadowMap[b]=X,I++}b++}else if(D.isRectAreaLight){let W=e.get(D);W.color.copy(U).multiplyScalar($),W.halfWidth.set(D.width*.5,0,0),W.halfHeight.set(0,D.height*.5,0),i.rectArea[A]=W,A++}else if(D.isPointLight){let W=e.get(D);if(W.color.copy(D.color).multiplyScalar(D.intensity),W.distance=D.distance,W.decay=D.decay,D.castShadow){let Q=D.shadow,ne=t.get(D);ne.shadowIntensity=Q.intensity,ne.shadowBias=Q.bias,ne.shadowNormalBias=Q.normalBias,ne.shadowRadius=Q.radius,ne.shadowMapSize=Q.mapSize,ne.shadowCameraNear=Q.camera.near,ne.shadowCameraFar=Q.camera.far,i.pointShadow[p]=ne,i.pointShadowMap[p]=X,i.pointShadowMatrix[p]=D.shadow.matrix,E++}i.point[p]=W,p++}else if(D.isHemisphereLight){let W=e.get(D);W.skyColor.copy(D.color).multiplyScalar($),W.groundColor.copy(D.groundColor).multiplyScalar($),i.hemi[S]=W,S++}}A>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=fe.LTC_FLOAT_1,i.rectAreaLTC2=fe.LTC_FLOAT_2):(i.rectAreaLTC1=fe.LTC_HALF_1,i.rectAreaLTC2=fe.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let O=i.hash;(O.sunLength!==h||O.directionalLength!==g||O.pointLength!==p||O.spotLength!==b||O.rectAreaLength!==A||O.hemiLength!==S||O.numSunShadows!==m||O.numDirectionalShadows!==M||O.numPointShadows!==E||O.numSpotShadows!==I||O.numSpotMaps!==_||O.numLightProbes!==R)&&(i.sun.length=h,i.directional.length=g,i.spot.length=b,i.rectArea.length=A,i.point.length=p,i.hemi.length=S,i.sunShadow.length=m,i.sunShadowMap.length=m,i.sunShadowMatrix.length=y,i.sunShadowCascade.length=y,i.directionalShadow.length=M,i.directionalShadowMap.length=M,i.directionalShadowMatrix.length=M,i.pointShadow.length=E,i.pointShadowMap.length=E,i.pointShadowMatrix.length=E,i.spotShadow.length=I,i.spotShadowMap.length=I,i.spotLightMatrix.length=I+_-T,i.spotLightMap.length=_,i.numSpotLightShadowsWithMaps=T,i.numLightProbes=R,O.sunLength=h,O.directionalLength=g,O.pointLength=p,O.spotLength=b,O.rectAreaLength=A,O.hemiLength=S,O.numSunShadows=m,O.numDirectionalShadows=M,O.numPointShadows=E,O.numSpotShadows=I,O.numSpotMaps=_,O.numLightProbes=R,i.version=zU++)}function c(l,u){let d=0,f=0,h=0,m=0,y=0,g=0,p=u.matrixWorldInverse;for(let b=0,A=l.length;b<A;b++){let S=l[b];if(S.isSunLight){let M=i.sun[d];M.direction.setFromMatrixPosition(S.matrixWorld),M.direction.transformDirection(p),d++}else if(S.isDirectionalLight){let M=i.directional[f];M.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),M.direction.sub(r),M.direction.transformDirection(p),f++}else if(S.isSpotLight){let M=i.spot[m];M.position.setFromMatrixPosition(S.matrixWorld),M.position.applyMatrix4(p),M.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),M.direction.sub(r),M.direction.transformDirection(p),m++}else if(S.isRectAreaLight){let M=i.rectArea[y];M.position.setFromMatrixPosition(S.matrixWorld),M.position.applyMatrix4(p),o.identity(),s.copy(S.matrixWorld),s.premultiply(p),o.extractRotation(s),M.halfWidth.set(S.width*.5,0,0),M.halfHeight.set(0,S.height*.5,0),M.halfWidth.applyMatrix4(o),M.halfHeight.applyMatrix4(o),y++}else if(S.isPointLight){let M=i.point[h];M.position.setFromMatrixPosition(S.matrixWorld),M.position.applyMatrix4(p),h++}else if(S.isHemisphereLight){let M=i.hemi[g];M.direction.setFromMatrixPosition(S.matrixWorld),M.direction.transformDirection(p),g++}}}return{setup:a,setupView:c,state:i}}function Mw(n){let e=new WU(n),t=[],i=[],r=[];function s(f){d.camera=f,t.length=0,i.length=0,r.length=0}function o(f){t.push(f)}function a(f){i.push(f)}function c(f){r.push(f)}function l(){e.setup(t)}function u(f){e.setupView(t,f)}let d={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:d,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function jU(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new Mw(n),e.set(r,[a])):s>=o.length?(a=new Mw(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var $U=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,qU=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,XU=[new F(1,0,0),new F(-1,0,0),new F(0,1,0),new F(0,-1,0),new F(0,0,1),new F(0,0,-1)],YU=[new F(0,-1,0),new F(0,-1,0),new F(0,0,1),new F(0,0,-1),new F(0,-1,0),new F(0,-1,0)],Ew=new Lt,Wl=new F,h_=new F;function ZU(n,e,t){let i=new El,r=new Pe,s=new Pe,o=new Ot,a=new wh,c=new Th,l={},u=t.maxTextureSize,d={[us]:Pn,[Pn]:us,[Qn]:Qn},f=new Kn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Pe},radius:{value:4}},vertexShader:$U,fragmentShader:qU}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let m=new pn;m.setAttribute("position",new fn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new En(m,f),g=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Ll;let p=this.type;this.render=function(E,I,_){if(g.enabled===!1||g.autoUpdate===!1&&g.needsUpdate===!1||E.length===0)return;this.type===_E&&(Re("WebGLShadowMap: PCFSoftShadowMap has been removed. Using PCFShadowMap instead."),this.type=Ll);let T=n.getRenderTarget(),R=n.getActiveCubeFace(),O=n.getActiveMipmapLevel(),C=n.state;C.setBlending(nr),C.buffers.depth.getReversed()===!0?C.buffers.color.setClear(0,0,0,0):C.buffers.color.setClear(1,1,1,1),C.buffers.depth.setTest(!0),C.setScissorTest(!1);let V=p!==this.type;V&&I.traverse(function(D){D.material&&(Array.isArray(D.material)?D.material.forEach(U=>U.needsUpdate=!0):D.material.needsUpdate=!0)});for(let D=0,U=E.length;D<U;D++){let $=E[D],B=$.shadow;if(B===void 0){Re("WebGLShadowMap:",$,"has no shadow.");continue}if(B.autoUpdate===!1&&B.needsUpdate===!1)continue;r.copy(B.mapSize);let X=B.getFrameExtents();r.multiply(X),s.copy(B.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/X.x),r.x=s.x*X.x,B.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/X.y),r.y=s.y*X.y,B.mapSize.y=s.y));let W=n.state.buffers.depth.getReversed();if(B.camera._reversedDepth=W,B.map===null||V===!0){if(B.map!==null&&(B.map.depthTexture!==null&&(B.map.depthTexture.dispose(),B.map.depthTexture=null),B.map.dispose()),this.type===Aa){if($.isPointLight){Re("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}B.map=new Un(r.x,r.y,{format:ps,type:Ui,minFilter:hn,magFilter:hn,generateMipmaps:!1}),B.map.texture.name=$.name+".shadowMap",B.map.depthTexture=new is(r.x,r.y,Fi),B.map.depthTexture.name=$.name+".shadowMapDepth",B.map.depthTexture.format=tr,B.map.depthTexture.compareFunction=null,B.map.depthTexture.minFilter=sn,B.map.depthTexture.magFilter=sn}else $.isPointLight?(B.map=new Cp(r.x),B.map.depthTexture=new Mh(r.x,Oi)):(B.map=new Un(r.x,r.y),B.map.depthTexture=new is(r.x,r.y,Oi)),B.map.depthTexture.name=$.name+".shadowMap",B.map.depthTexture.format=tr,this.type===Ll?(B.map.depthTexture.compareFunction=W?Mp:bp,B.map.depthTexture.minFilter=hn,B.map.depthTexture.magFilter=hn):(B.map.depthTexture.compareFunction=null,B.map.depthTexture.minFilter=sn,B.map.depthTexture.magFilter=sn);B.camera.updateProjectionMatrix()}B.map.isWebGLCubeRenderTarget!==!0&&(B.map.width!==r.x||B.map.height!==r.y)&&B.map.setSize(r.x,r.y);let Q=B.map.isWebGLCubeRenderTarget?6:B.getViewportCount();$.isPointLight!==!0&&B.updateMatrices($,_);for(let ne=0;ne<Q;ne++){let Ie=B.getCamera(ne);if($.isPointLight){let Te=B.camera,Ct=B.matrix,ot=$.distance||Te.far;ot!==Te.far&&(Te.far=ot,Te.updateProjectionMatrix()),Wl.setFromMatrixPosition($.matrixWorld),Te.position.copy(Wl),h_.copy(Te.position),h_.add(XU[ne]),Te.up.copy(YU[ne]),Te.lookAt(h_),Te.updateMatrixWorld(),Ct.makeTranslation(-Wl.x,-Wl.y,-Wl.z),Ew.multiplyMatrices(Te.projectionMatrix,Te.matrixWorldInverse),B._frustum.setFromProjectionMatrix(Ew,Te.coordinateSystem,Te.reversedDepth)}if(B.map.isWebGLCubeRenderTarget)n.setRenderTarget(B.map,ne),n.clear();else{ne===0&&(n.setRenderTarget(B.map),n.clear());let Te=B.getViewport(ne);o.set(s.x*Te.x,s.y*Te.y,s.x*Te.z,s.y*Te.w),C.viewport(o)}i=B.getFrustum(ne),S(I,_,Ie,$,this.type)}B.isPointLightShadow!==!0&&this.type===Aa&&b(B,_),B.needsUpdate=!1}p=this.type,g.needsUpdate=!1,n.setRenderTarget(T,R,O)};function b(E,I){let _=e.update(y);f.defines.VSM_SAMPLES!==E.blurSamples&&(f.defines.VSM_SAMPLES=E.blurSamples,h.defines.VSM_SAMPLES=E.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),E.mapPass===null?E.mapPass=new Un(r.x,r.y,{format:ps,type:Ui}):(E.mapPass.width!==E.map.width||E.mapPass.height!==E.map.height)&&E.mapPass.setSize(E.map.width,E.map.height),f.uniforms.shadow_pass.value=E.map.depthTexture,f.uniforms.resolution.value.set(E.map.width,E.map.height),f.uniforms.radius.value=E.radius,n.setRenderTarget(E.mapPass),n.clear(),n.renderBufferDirect(I,null,_,f,y,null),h.uniforms.shadow_pass.value=E.mapPass.texture,h.uniforms.resolution.value.set(E.map.width,E.map.height),h.uniforms.radius.value=E.radius,n.setRenderTarget(E.map),n.clear(),n.renderBufferDirect(I,null,_,h,y,null)}function A(E,I,_,T){let R=null,O=_.isPointLight===!0?E.customDistanceMaterial:E.customDepthMaterial;if(O!==void 0)R=O;else if(R=_.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let C=R.uuid,V=I.uuid,D=l[C];D===void 0&&(D={},l[C]=D);let U=D[V];U===void 0&&(U=R.clone(),D[V]=U,I.addEventListener("dispose",M)),R=U}if(R.visible=I.visible,R.wireframe=I.wireframe,T===Aa?R.side=I.shadowSide!==null?I.shadowSide:I.side:R.side=I.shadowSide!==null?I.shadowSide:d[I.side],R.alphaMap=I.alphaMap,R.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,R.map=I.map,R.clipShadows=I.clipShadows,R.clippingPlanes=I.clippingPlanes,R.clipIntersection=I.clipIntersection,R.displacementMap=I.displacementMap,R.displacementScale=I.displacementScale,R.displacementBias=I.displacementBias,R.wireframeLinewidth=I.wireframeLinewidth,R.linewidth=I.linewidth,_.isPointLight===!0&&R.isMeshDistanceMaterial===!0){let C=n.properties.get(R);C.light=_}return R}function S(E,I,_,T,R){if(E.visible===!1)return;if(E.layers.test(I.layers)&&(E.isMesh||E.isLine||E.isPoints)&&(E.castShadow||E.receiveShadow&&R===Aa)&&(!E.frustumCulled||E.intersectsFrustum(i))){E.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,E.matrixWorld);let V=e.update(E),D=E.material;if(Array.isArray(D)){let U=V.groups;for(let $=0,B=U.length;$<B;$++){let X=U[$],W=D[X.materialIndex];if(W&&W.visible){let Q=A(E,W,T,R);E.onBeforeShadow(n,E,I,_,V,Q,X),n.renderBufferDirect(_,null,V,Q,E,X),E.onAfterShadow(n,E,I,_,V,Q,X)}}}else if(D.visible){let U=A(E,D,T,R);E.onBeforeShadow(n,E,I,_,V,U,null),n.renderBufferDirect(_,null,V,U,E,null),E.onAfterShadow(n,E,I,_,V,U,null)}}let C=E.children;for(let V=0,D=C.length;V<D;V++)S(C[V],I,_,T,R)}function M(E){E.target.removeEventListener("dispose",M);for(let _ in l){let T=l[_],R=E.target.uuid;R in T&&(T[R].dispose(),delete T[R])}}}function KU(n,e){function t(){let P=!1,le=new Ot,K=null,ue=new Ot(0,0,0,0);return{setMask:function(me){K!==me&&!P&&(n.colorMask(me,me,me,me),K=me)},setLocked:function(me){P=me},setClear:function(me,te,Ce,be,At){At===!0&&(me*=be,te*=be,Ce*=be),le.set(me,te,Ce,be),ue.equals(le)===!1&&(n.clearColor(me,te,Ce,be),ue.copy(le))},reset:function(){P=!1,K=null,ue.set(-1,0,0,0)}}}function i(){let P=!1,le=!1,K=null,ue=null,me=null;return{setReversed:function(te){if(le!==te){let Ce=e.get("EXT_clip_control");te?Ce.clipControlEXT(Ce.LOWER_LEFT_EXT,Ce.ZERO_TO_ONE_EXT):Ce.clipControlEXT(Ce.LOWER_LEFT_EXT,Ce.NEGATIVE_ONE_TO_ONE_EXT),le=te;let be=me;me=null,this.setClear(be)}},getReversed:function(){return le},setTest:function(te){te?ee(n.DEPTH_TEST):_e(n.DEPTH_TEST)},setMask:function(te){K!==te&&!P&&(n.depthMask(te),K=te)},setFunc:function(te){if(le&&(te=QE[te]),ue!==te){switch(te){case sh:n.depthFunc(n.NEVER);break;case oh:n.depthFunc(n.ALWAYS);break;case ah:n.depthFunc(n.LESS);break;case Sa:n.depthFunc(n.LEQUAL);break;case ch:n.depthFunc(n.EQUAL);break;case lh:n.depthFunc(n.GEQUAL);break;case uh:n.depthFunc(n.GREATER);break;case dh:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ue=te}},setLocked:function(te){P=te},setClear:function(te){me!==te&&(me=te,le&&(te=1-te),n.clearDepth(te))},reset:function(){P=!1,K=null,ue=null,me=null,le=!1}}}function r(){let P=!1,le=null,K=null,ue=null,me=null,te=null,Ce=null,be=null,At=null;return{setTest:function(mt){P||(mt?ee(n.STENCIL_TEST):_e(n.STENCIL_TEST))},setMask:function(mt){le!==mt&&!P&&(n.stencilMask(mt),le=mt)},setFunc:function(mt,fi,ki){(K!==mt||ue!==fi||me!==ki)&&(n.stencilFunc(mt,fi,ki),K=mt,ue=fi,me=ki)},setOp:function(mt,fi,ki){(te!==mt||Ce!==fi||be!==ki)&&(n.stencilOp(mt,fi,ki),te=mt,Ce=fi,be=ki)},setLocked:function(mt){P=mt},setClear:function(mt){At!==mt&&(n.clearStencil(mt),At=mt)},reset:function(){P=!1,le=null,K=null,ue=null,me=null,te=null,Ce=null,be=null,At=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f={},h=new WeakMap,m=[],y=null,g=!1,p=null,b=null,A=null,S=null,M=null,E=null,I=null,_=new st(0,0,0),T=0,R=!1,O=null,C=null,V=null,D=null,U=null,$=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),B=!1,X=0,W=n.getParameter(n.VERSION);W.indexOf("WebGL")!==-1?(X=parseFloat(/^WebGL (\d)/.exec(W)[1]),B=X>=1):W.indexOf("OpenGL ES")!==-1&&(X=parseFloat(/^OpenGL ES (\d)/.exec(W)[1]),B=X>=2);let Q=null,ne={},Ie=n.getParameter(n.SCISSOR_BOX),Te=n.getParameter(n.VIEWPORT),Ct=new Ot().fromArray(Ie),ot=new Ot().fromArray(Te);function dt(P,le,K,ue){let me=new Uint8Array(4),te=n.createTexture();n.bindTexture(P,te),n.texParameteri(P,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(P,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Ce=0;Ce<K;Ce++)P===n.TEXTURE_3D||P===n.TEXTURE_2D_ARRAY?n.texImage3D(le,0,n.RGBA,1,1,ue,0,n.RGBA,n.UNSIGNED_BYTE,me):n.texImage2D(le+Ce,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,me);return te}let Y={};Y[n.TEXTURE_2D]=dt(n.TEXTURE_2D,n.TEXTURE_2D,1),Y[n.TEXTURE_CUBE_MAP]=dt(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),Y[n.TEXTURE_2D_ARRAY]=dt(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),Y[n.TEXTURE_3D]=dt(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ee(n.DEPTH_TEST),o.setFunc(Sa),et(!1),Nt(Ny),ee(n.CULL_FACE),ut(nr);function ee(P){u[P]!==!0&&(n.enable(P),u[P]=!0)}function _e(P){u[P]!==!1&&(n.disable(P),u[P]=!1)}function Ve(P,le){return f[P]!==le?(n.bindFramebuffer(P,le),f[P]=le,P===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=le),P===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=le),!0):!1}function ve(P,le){let K=m,ue=!1;if(P){K=h.get(le),K===void 0&&(K=[],h.set(le,K));let me=P.textures;if(K.length!==me.length||K[0]!==n.COLOR_ATTACHMENT0){for(let te=0,Ce=me.length;te<Ce;te++)K[te]=n.COLOR_ATTACHMENT0+te;K.length=me.length,ue=!0}}else K[0]!==n.BACK&&(K[0]=n.BACK,ue=!0);ue&&n.drawBuffers(K)}function Ye(P){return y!==P?(n.useProgram(P),y=P,!0):!1}let $t={[io]:n.FUNC_ADD,[SE]:n.FUNC_SUBTRACT,[bE]:n.FUNC_REVERSE_SUBTRACT};$t[ME]=n.MIN,$t[EE]=n.MAX;let Je={[wE]:n.ZERO,[TE]:n.ONE,[CE]:n.SRC_COLOR,[Fy]:n.SRC_ALPHA,[PE]:n.SRC_ALPHA_SATURATE,[RE]:n.DST_COLOR,[AE]:n.DST_ALPHA,[IE]:n.ONE_MINUS_SRC_COLOR,[Uy]:n.ONE_MINUS_SRC_ALPHA,[NE]:n.ONE_MINUS_DST_COLOR,[DE]:n.ONE_MINUS_DST_ALPHA,[LE]:n.CONSTANT_COLOR,[OE]:n.ONE_MINUS_CONSTANT_COLOR,[FE]:n.CONSTANT_ALPHA,[UE]:n.ONE_MINUS_CONSTANT_ALPHA};function ut(P,le,K,ue,me,te,Ce,be,At,mt){if(P===nr){g===!0&&(_e(n.BLEND),g=!1);return}if(g===!1&&(ee(n.BLEND),g=!0),P!==xE){if(P!==p||mt!==R){if((b!==io||M!==io)&&(n.blendEquation(n.FUNC_ADD),b=io,M=io),mt)switch(P){case Da:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Py:n.blendFunc(n.ONE,n.ONE);break;case Ly:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Oy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Le("WebGLState: Invalid blending: ",P);break}else switch(P){case Da:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Py:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Ly:Le("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Oy:Le("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Le("WebGLState: Invalid blending: ",P);break}A=null,S=null,E=null,I=null,_.set(0,0,0),T=0,p=P,R=mt}return}me=me||le,te=te||K,Ce=Ce||ue,(le!==b||me!==M)&&(n.blendEquationSeparate($t[le],$t[me]),b=le,M=me),(K!==A||ue!==S||te!==E||Ce!==I)&&(n.blendFuncSeparate(Je[K],Je[ue],Je[te],Je[Ce]),A=K,S=ue,E=te,I=Ce),(be.equals(_)===!1||At!==T)&&(n.blendColor(be.r,be.g,be.b,At),_.copy(be),T=At),p=P,R=!1}function It(P,le){P.side===Qn?_e(n.CULL_FACE):ee(n.CULL_FACE);let K=P.side===Pn;le&&(K=!K),et(K),P.blending===Da&&P.transparent===!1?ut(nr):ut(P.blending,P.blendEquation,P.blendSrc,P.blendDst,P.blendEquationAlpha,P.blendSrcAlpha,P.blendDstAlpha,P.blendColor,P.blendAlpha,P.premultipliedAlpha),o.setFunc(P.depthFunc),o.setTest(P.depthTest),o.setMask(P.depthWrite),s.setMask(P.colorWrite);let ue=P.stencilWrite;a.setTest(ue),ue&&(a.setMask(P.stencilWriteMask),a.setFunc(P.stencilFunc,P.stencilRef,P.stencilFuncMask),a.setOp(P.stencilFail,P.stencilZFail,P.stencilZPass)),Ln(P.polygonOffset,P.polygonOffsetFactor,P.polygonOffsetUnits),P.alphaToCoverage===!0?ee(n.SAMPLE_ALPHA_TO_COVERAGE):_e(n.SAMPLE_ALPHA_TO_COVERAGE)}function et(P){O!==P&&(P?n.frontFace(n.CW):n.frontFace(n.CCW),O=P)}function Nt(P){P!==vE?(ee(n.CULL_FACE),P!==C&&(P===Ny?n.cullFace(n.BACK):P===yE?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):_e(n.CULL_FACE),C=P}function en(P){P!==V&&(B&&n.lineWidth(P),V=P)}function Ln(P,le,K){P?(ee(n.POLYGON_OFFSET_FILL),(D!==le||U!==K)&&(D=le,U=K,o.getReversed()&&(le=-le),n.polygonOffset(le,K))):_e(n.POLYGON_OFFSET_FILL)}function Pt(P){P?ee(n.SCISSOR_TEST):_e(n.SCISSOR_TEST)}function Ht(P){P===void 0&&(P=n.TEXTURE0+$-1),Q!==P&&(n.activeTexture(P),Q=P)}function L(P,le,K){K===void 0&&(Q===null?K=n.TEXTURE0+$-1:K=Q);let ue=ne[K];ue===void 0&&(ue={type:void 0,texture:void 0},ne[K]=ue),(ue.type!==P||ue.texture!==le)&&(Q!==K&&(n.activeTexture(K),Q=K),n.bindTexture(P,le||Y[P]),ue.type=P,ue.texture=le)}function mn(){let P=ne[Q];P!==void 0&&P.type!==void 0&&(n.bindTexture(P.type,null),P.type=void 0,P.texture=void 0)}function xt(){try{n.compressedTexImage2D(...arguments)}catch(P){Le("WebGLState:",P)}}function w(){try{n.compressedTexImage3D(...arguments)}catch(P){Le("WebGLState:",P)}}function v(){try{n.texSubImage2D(...arguments)}catch(P){Le("WebGLState:",P)}}function k(){try{n.texSubImage3D(...arguments)}catch(P){Le("WebGLState:",P)}}function G(){try{n.compressedTexSubImage2D(...arguments)}catch(P){Le("WebGLState:",P)}}function q(){try{n.compressedTexSubImage3D(...arguments)}catch(P){Le("WebGLState:",P)}}function ie(){try{n.texStorage2D(...arguments)}catch(P){Le("WebGLState:",P)}}function se(){try{n.texStorage3D(...arguments)}catch(P){Le("WebGLState:",P)}}function Z(){try{n.texImage2D(...arguments)}catch(P){Le("WebGLState:",P)}}function J(){try{n.texImage3D(...arguments)}catch(P){Le("WebGLState:",P)}}function ae(P){return d[P]!==void 0?d[P]:n.getParameter(P)}function Ee(P,le){d[P]!==le&&(n.pixelStorei(P,le),d[P]=le)}function de(P){Ct.equals(P)===!1&&(n.scissor(P.x,P.y,P.z,P.w),Ct.copy(P))}function ce(P){ot.equals(P)===!1&&(n.viewport(P.x,P.y,P.z,P.w),ot.copy(P))}function we(P,le){let K=l.get(le);K===void 0&&(K=new WeakMap,l.set(le,K));let ue=K.get(P);ue===void 0&&(ue=n.getUniformBlockIndex(le,P.name),K.set(P,ue))}function Ne(P,le){let ue=l.get(le).get(P);c.get(le)!==ue&&(n.uniformBlockBinding(le,ue,P.__bindingPointIndex),c.set(le,ue))}function We(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},d={},Q=null,ne={},f={},h=new WeakMap,m=[],y=null,g=!1,p=null,b=null,A=null,S=null,M=null,E=null,I=null,_=new st(0,0,0),T=0,R=!1,O=null,C=null,V=null,D=null,U=null,Ct.set(0,0,n.canvas.width,n.canvas.height),ot.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ee,disable:_e,bindFramebuffer:Ve,drawBuffers:ve,useProgram:Ye,setBlending:ut,setMaterial:It,setFlipSided:et,setCullFace:Nt,setLineWidth:en,setPolygonOffset:Ln,setScissorTest:Pt,activeTexture:Ht,bindTexture:L,unbindTexture:mn,compressedTexImage2D:xt,compressedTexImage3D:w,texImage2D:Z,texImage3D:J,pixelStorei:Ee,getParameter:ae,updateUBOMapping:we,uniformBlockBinding:Ne,texStorage2D:ie,texStorage3D:se,texSubImage2D:v,texSubImage3D:k,compressedTexSubImage2D:G,compressedTexSubImage3D:q,scissor:de,viewport:ce,reset:We}}function JU(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new Pe,u=new WeakMap,d=new Set,f,h=new WeakMap,m=!1;try{m=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function y(w,v){return m?new OffscreenCanvas(w,v):pl("canvas")}function g(w,v,k){let G=1,q=xt(w);if((q.width>k||q.height>k)&&(G=k/Math.max(q.width,q.height)),G<1)if(typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&w instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&w instanceof ImageBitmap||typeof VideoFrame<"u"&&w instanceof VideoFrame){let ie=Math.floor(G*q.width),se=Math.floor(G*q.height);f===void 0&&(f=y(ie,se));let Z=v?y(ie,se):f;return Z.width=ie,Z.height=se,Z.getContext("2d").drawImage(w,0,0,ie,se),Re("WebGLRenderer: Texture has been resized from ("+q.width+"x"+q.height+") to ("+ie+"x"+se+")."),Z}else return"data"in w&&Re("WebGLRenderer: Image in DataTexture is too big ("+q.width+"x"+q.height+")."),w;return w}function p(w){return w.generateMipmaps}function b(w){n.generateMipmap(w)}function A(w){return w.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:w.isWebGL3DRenderTarget?n.TEXTURE_3D:w.isWebGLArrayRenderTarget||w.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function S(w,v,k,G,q,ie=!1){if(w!==null){if(n[w]!==void 0)return n[w];Re("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+w+"'")}let se;G&&(se=e.get("EXT_texture_norm16"),se||Re("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let Z=v;if(v===n.RED&&(k===n.FLOAT&&(Z=n.R32F),k===n.HALF_FLOAT&&(Z=n.R16F),k===n.UNSIGNED_BYTE&&(Z=n.R8),k===n.UNSIGNED_SHORT&&se&&(Z=se.R16_EXT),k===n.SHORT&&se&&(Z=se.R16_SNORM_EXT)),v===n.RED_INTEGER&&(k===n.UNSIGNED_BYTE&&(Z=n.R8UI),k===n.UNSIGNED_SHORT&&(Z=n.R16UI),k===n.UNSIGNED_INT&&(Z=n.R32UI),k===n.BYTE&&(Z=n.R8I),k===n.SHORT&&(Z=n.R16I),k===n.INT&&(Z=n.R32I)),v===n.RG&&(k===n.FLOAT&&(Z=n.RG32F),k===n.HALF_FLOAT&&(Z=n.RG16F),k===n.UNSIGNED_BYTE&&(Z=n.RG8),k===n.UNSIGNED_SHORT&&se&&(Z=se.RG16_EXT),k===n.SHORT&&se&&(Z=se.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(k===n.UNSIGNED_BYTE&&(Z=n.RG8UI),k===n.UNSIGNED_SHORT&&(Z=n.RG16UI),k===n.UNSIGNED_INT&&(Z=n.RG32UI),k===n.BYTE&&(Z=n.RG8I),k===n.SHORT&&(Z=n.RG16I),k===n.INT&&(Z=n.RG32I)),v===n.RGB_INTEGER&&(k===n.UNSIGNED_BYTE&&(Z=n.RGB8UI),k===n.UNSIGNED_SHORT&&(Z=n.RGB16UI),k===n.UNSIGNED_INT&&(Z=n.RGB32UI),k===n.BYTE&&(Z=n.RGB8I),k===n.SHORT&&(Z=n.RGB16I),k===n.INT&&(Z=n.RGB32I)),v===n.RGBA_INTEGER&&(k===n.UNSIGNED_BYTE&&(Z=n.RGBA8UI),k===n.UNSIGNED_SHORT&&(Z=n.RGBA16UI),k===n.UNSIGNED_INT&&(Z=n.RGBA32UI),k===n.BYTE&&(Z=n.RGBA8I),k===n.SHORT&&(Z=n.RGBA16I),k===n.INT&&(Z=n.RGBA32I)),v===n.RGB&&(k===n.UNSIGNED_SHORT&&se&&(Z=se.RGB16_EXT),k===n.SHORT&&se&&(Z=se.RGB16_SNORM_EXT),k===n.UNSIGNED_INT_5_9_9_9_REV&&(Z=n.RGB9_E5),k===n.UNSIGNED_INT_10F_11F_11F_REV&&(Z=n.R11F_G11F_B10F)),v===n.RGBA){let J=ie?fl:rt.getTransfer(q);k===n.FLOAT&&(Z=n.RGBA32F),k===n.HALF_FLOAT&&(Z=n.RGBA16F),k===n.UNSIGNED_BYTE&&(Z=J===_t?n.SRGB8_ALPHA8:n.RGBA8),k===n.UNSIGNED_SHORT&&se&&(Z=se.RGBA16_EXT),k===n.SHORT&&se&&(Z=se.RGBA16_SNORM_EXT),k===n.UNSIGNED_SHORT_4_4_4_4&&(Z=n.RGBA4),k===n.UNSIGNED_SHORT_5_5_5_1&&(Z=n.RGB5_A1)}return(Z===n.R16F||Z===n.R32F||Z===n.RG16F||Z===n.RG32F||Z===n.RGBA16F||Z===n.RGBA32F)&&e.get("EXT_color_buffer_float"),Z}function M(w,v){let k;return w?v===null||v===Oi||v===Na?k=n.DEPTH24_STENCIL8:v===Fi?k=n.DEPTH32F_STENCIL8:v===Ra&&(k=n.DEPTH24_STENCIL8,Re("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Oi||v===Na?k=n.DEPTH_COMPONENT24:v===Fi?k=n.DEPTH_COMPONENT32F:v===Ra&&(k=n.DEPTH_COMPONENT16),k}function E(w,v){return p(w)===!0||w.isFramebufferTexture&&w.minFilter!==sn&&w.minFilter!==hn?Math.log2(Math.max(v.width,v.height))+1:w.mipmaps!==void 0&&w.mipmaps.length>0?w.mipmaps.length:w.isCompressedTexture&&Array.isArray(w.image)?v.mipmaps.length:1}function I(w){let v=w.target;v.removeEventListener("dispose",I),T(v),v.isVideoTexture&&u.delete(v),v.isHTMLTexture&&d.delete(v)}function _(w){let v=w.target;v.removeEventListener("dispose",_),O(v)}function T(w){let v=i.get(w);if(v.__webglInit===void 0)return;let k=w.source,G=h.get(k);if(G){let q=G[v.__cacheKey];q.usedTimes--,q.usedTimes===0&&R(w),Object.keys(G).length===0&&h.delete(k)}i.remove(w)}function R(w){let v=i.get(w);n.deleteTexture(v.__webglTexture);let k=w.source,G=h.get(k);delete G[v.__cacheKey],o.memory.textures--}function O(w){let v=i.get(w);if(w.depthTexture&&(w.depthTexture.dispose(),i.remove(w.depthTexture)),w.isWebGLCubeRenderTarget)for(let G=0;G<6;G++){if(Array.isArray(v.__webglFramebuffer[G]))for(let q=0;q<v.__webglFramebuffer[G].length;q++)n.deleteFramebuffer(v.__webglFramebuffer[G][q]);else n.deleteFramebuffer(v.__webglFramebuffer[G]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[G])}else{if(Array.isArray(v.__webglFramebuffer))for(let G=0;G<v.__webglFramebuffer.length;G++)n.deleteFramebuffer(v.__webglFramebuffer[G]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let G=0;G<v.__webglColorRenderbuffer.length;G++)v.__webglColorRenderbuffer[G]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[G]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let k=w.textures;for(let G=0,q=k.length;G<q;G++){let ie=i.get(k[G]);ie.__webglTexture&&(n.deleteTexture(ie.__webglTexture),o.memory.textures--),i.remove(k[G])}i.remove(w)}let C=0;function V(){C=0}function D(){return C}function U(w){C=w}function $(){let w=C;return w>=r.maxTextures&&Re("WebGLTextures: Trying to use "+(w+1)+" texture units while this GPU supports only "+r.maxTextures),C+=1,w}function B(w){let v=[];return v.push(w.wrapS),v.push(w.wrapT),v.push(w.wrapR||0),v.push(w.magFilter),v.push(w.minFilter),v.push(w.anisotropy),v.push(w.internalFormat),v.push(w.format),v.push(w.type),v.push(w.generateMipmaps),v.push(w.premultiplyAlpha),v.push(w.flipY),v.push(w.unpackAlignment),v.push(w.colorSpace),v.join()}function X(w,v){let k=i.get(w);if(w.isVideoTexture&&L(w),w.isRenderTargetTexture===!1&&w.isExternalTexture!==!0&&w.version>0&&k.__version!==w.version){let G=w.image;if(G===null)Re("WebGLRenderer: Texture marked for update but no image data found.");else if(G.complete===!1)Re("WebGLRenderer: Texture marked for update but image is incomplete");else{_e(k,w,v);return}}else w.isExternalTexture&&(k.__webglTexture=w.sourceTexture?w.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,k.__webglTexture,n.TEXTURE0+v)}function W(w,v){let k=i.get(w);if(w.isRenderTargetTexture===!1&&w.version>0&&k.__version!==w.version){_e(k,w,v);return}else w.isExternalTexture&&(k.__webglTexture=w.sourceTexture?w.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,k.__webglTexture,n.TEXTURE0+v)}function Q(w,v){let k=i.get(w);if(w.isRenderTargetTexture===!1&&w.version>0&&k.__version!==w.version){_e(k,w,v);return}t.bindTexture(n.TEXTURE_3D,k.__webglTexture,n.TEXTURE0+v)}function ne(w,v){let k=i.get(w);if(w.isCubeDepthTexture!==!0&&w.version>0&&k.__version!==w.version){Ve(k,w,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,k.__webglTexture,n.TEXTURE0+v)}let Ie={[fh]:n.REPEAT,[er]:n.CLAMP_TO_EDGE,[hh]:n.MIRRORED_REPEAT},Te={[sn]:n.NEAREST,[VE]:n.NEAREST_MIPMAP_NEAREST,[Fl]:n.NEAREST_MIPMAP_LINEAR,[hn]:n.LINEAR,[Bh]:n.LINEAR_MIPMAP_NEAREST,[fs]:n.LINEAR_MIPMAP_LINEAR},Ct={[WE]:n.NEVER,[YE]:n.ALWAYS,[jE]:n.LESS,[bp]:n.LEQUAL,[$E]:n.EQUAL,[Mp]:n.GEQUAL,[qE]:n.GREATER,[XE]:n.NOTEQUAL};function ot(w,v){if(v.type===Fi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===hn||v.magFilter===Bh||v.magFilter===Fl||v.magFilter===fs||v.minFilter===hn||v.minFilter===Bh||v.minFilter===Fl||v.minFilter===fs)&&Re("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(w,n.TEXTURE_WRAP_S,Ie[v.wrapS]),n.texParameteri(w,n.TEXTURE_WRAP_T,Ie[v.wrapT]),(w===n.TEXTURE_3D||w===n.TEXTURE_2D_ARRAY)&&n.texParameteri(w,n.TEXTURE_WRAP_R,Ie[v.wrapR]),n.texParameteri(w,n.TEXTURE_MAG_FILTER,Te[v.magFilter]),n.texParameteri(w,n.TEXTURE_MIN_FILTER,Te[v.minFilter]),v.compareFunction&&(n.texParameteri(w,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(w,n.TEXTURE_COMPARE_FUNC,Ct[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===sn||v.minFilter!==Fl&&v.minFilter!==fs||v.type===Fi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let k=e.get("EXT_texture_filter_anisotropic");n.texParameterf(w,k.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function dt(w,v){let k=!1;w.__webglInit===void 0&&(w.__webglInit=!0,v.addEventListener("dispose",I));let G=v.source,q=h.get(G);q===void 0&&(q={},h.set(G,q));let ie=B(v);if(ie!==w.__cacheKey){q[ie]===void 0&&(q[ie]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,k=!0),q[ie].usedTimes++;let se=q[w.__cacheKey];se!==void 0&&(q[w.__cacheKey].usedTimes--,se.usedTimes===0&&R(v)),w.__cacheKey=ie,w.__webglTexture=q[ie].texture}return k}function Y(w,v,k){return Math.floor(Math.floor(w/k)/v)}function ee(w,v,k,G){let ie=w.updateRanges;if(ie.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,k,G,v.data);else{ie.sort((Ee,de)=>Ee.start-de.start);let se=0;for(let Ee=1;Ee<ie.length;Ee++){let de=ie[se],ce=ie[Ee],we=de.start+de.count,Ne=Y(ce.start,v.width,4),We=Y(de.start,v.width,4);ce.start<=we+1&&Ne===We&&Y(ce.start+ce.count-1,v.width,4)===Ne?de.count=Math.max(de.count,ce.start+ce.count-de.start):(++se,ie[se]=ce)}ie.length=se+1;let Z=t.getParameter(n.UNPACK_ROW_LENGTH),J=t.getParameter(n.UNPACK_SKIP_PIXELS),ae=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let Ee=0,de=ie.length;Ee<de;Ee++){let ce=ie[Ee],we=Math.floor(ce.start/4),Ne=Math.ceil(ce.count/4),We=we%v.width,P=Math.floor(we/v.width),le=Ne,K=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,We),t.pixelStorei(n.UNPACK_SKIP_ROWS,P),t.texSubImage2D(n.TEXTURE_2D,0,We,P,le,K,k,G,v.data)}w.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,Z),t.pixelStorei(n.UNPACK_SKIP_PIXELS,J),t.pixelStorei(n.UNPACK_SKIP_ROWS,ae)}}function _e(w,v,k){let G=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(G=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(G=n.TEXTURE_3D);let q=dt(w,v),ie=v.source;t.bindTexture(G,w.__webglTexture,n.TEXTURE0+k);let se=i.get(ie);if(ie.version!==se.__version||q===!0){if(t.activeTexture(n.TEXTURE0+k),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let K=rt.getPrimaries(rt.workingColorSpace),ue=v.colorSpace===Ir?null:rt.getPrimaries(v.colorSpace),me=v.colorSpace===Ir||K===ue?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,me)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let J=g(v.image,!1,r.maxTextureSize);J=mn(v,J);let ae=s.convert(v.format,v.colorSpace),Ee=s.convert(v.type),de=S(v.internalFormat,ae,Ee,v.normalized,v.colorSpace,v.isVideoTexture);ot(G,v);let ce,we=v.mipmaps,Ne=v.isVideoTexture!==!0,We=se.__version===void 0||q===!0,P=ie.dataReady,le=E(v,J);if(v.isDepthTexture)de=M(v.format===hs,v.type),We&&(Ne?t.texStorage2D(n.TEXTURE_2D,1,de,J.width,J.height):t.texImage2D(n.TEXTURE_2D,0,de,J.width,J.height,0,ae,Ee,null));else if(v.isDataTexture)if(we.length>0){Ne&&We&&t.texStorage2D(n.TEXTURE_2D,le,de,we[0].width,we[0].height);for(let K=0,ue=we.length;K<ue;K++)ce=we[K],Ne?P&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,ce.width,ce.height,ae,Ee,ce.data):t.texImage2D(n.TEXTURE_2D,K,de,ce.width,ce.height,0,ae,Ee,ce.data);v.generateMipmaps=!1}else Ne?(We&&t.texStorage2D(n.TEXTURE_2D,le,de,J.width,J.height),P&&ee(v,J,ae,Ee)):t.texImage2D(n.TEXTURE_2D,0,de,J.width,J.height,0,ae,Ee,J.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){Ne&&We&&t.texStorage3D(n.TEXTURE_2D_ARRAY,le,de,we[0].width,we[0].height,J.depth);for(let K=0,ue=we.length;K<ue;K++)if(ce=we[K],v.format!==di)if(ae!==null)if(Ne){if(P)if(v.layerUpdates.size>0){let me=a_(ce.width,ce.height,v.format,v.type);for(let te of v.layerUpdates){let Ce=ce.data.subarray(te*me/ce.data.BYTES_PER_ELEMENT,(te+1)*me/ce.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,te,ce.width,ce.height,1,ae,Ce)}}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,ce.width,ce.height,J.depth,ae,ce.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,K,de,ce.width,ce.height,J.depth,0,ce.data,0,0);else Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Ne?P&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,K,0,0,0,ce.width,ce.height,J.depth,ae,Ee,ce.data):t.texImage3D(n.TEXTURE_2D_ARRAY,K,de,ce.width,ce.height,J.depth,0,ae,Ee,ce.data);v.layerUpdates.size>0&&v.clearLayerUpdates()}else{Ne&&We&&t.texStorage2D(n.TEXTURE_2D,le,de,we[0].width,we[0].height);for(let K=0,ue=we.length;K<ue;K++)ce=we[K],v.format!==di?ae!==null?Ne?P&&t.compressedTexSubImage2D(n.TEXTURE_2D,K,0,0,ce.width,ce.height,ae,ce.data):t.compressedTexImage2D(n.TEXTURE_2D,K,de,ce.width,ce.height,0,ce.data):Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Ne?P&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,ce.width,ce.height,ae,Ee,ce.data):t.texImage2D(n.TEXTURE_2D,K,de,ce.width,ce.height,0,ae,Ee,ce.data)}else if(v.isDataArrayTexture)if(Ne){if(We&&t.texStorage3D(n.TEXTURE_2D_ARRAY,le,de,J.width,J.height,J.depth),P)if(v.layerUpdates.size>0){let K=a_(J.width,J.height,v.format,v.type);for(let ue of v.layerUpdates){let me=J.data.subarray(ue*K/J.data.BYTES_PER_ELEMENT,(ue+1)*K/J.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,ue,J.width,J.height,1,ae,Ee,me)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,J.width,J.height,J.depth,ae,Ee,J.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,de,J.width,J.height,J.depth,0,ae,Ee,J.data);else if(v.isData3DTexture)Ne?(We&&t.texStorage3D(n.TEXTURE_3D,le,de,J.width,J.height,J.depth),P&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,J.width,J.height,J.depth,ae,Ee,J.data)):t.texImage3D(n.TEXTURE_3D,0,de,J.width,J.height,J.depth,0,ae,Ee,J.data);else if(v.isFramebufferTexture){if(We)if(Ne)t.texStorage2D(n.TEXTURE_2D,le,de,J.width,J.height);else{let K=J.width,ue=J.height;for(let me=0;me<le;me++)t.texImage2D(n.TEXTURE_2D,me,de,K,ue,0,ae,Ee,null),K>>=1,ue>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let K=n.canvas;if(K.hasAttribute("layoutsubtree")||K.setAttribute("layoutsubtree","true"),J.parentNode!==K){K.appendChild(J),d.add(v),K.onpaint=ue=>{let me=ue.changedElements;for(let te of d)me.includes(te.image)&&(te.needsUpdate=!0)},K.requestPaint();return}if(n.texElementImage2D.length===3)n.texElementImage2D(n.TEXTURE_2D,n.RGBA8,J);else{let me=n.RGBA,te=n.RGBA,Ce=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,0,me,te,Ce,J)}n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(we.length>0){if(Ne&&We){let K=xt(we[0]);t.texStorage2D(n.TEXTURE_2D,le,de,K.width,K.height)}for(let K=0,ue=we.length;K<ue;K++)ce=we[K],Ne?P&&t.texSubImage2D(n.TEXTURE_2D,K,0,0,ae,Ee,ce):t.texImage2D(n.TEXTURE_2D,K,de,ae,Ee,ce);v.generateMipmaps=!1}else if(Ne){if(We){let K=xt(J);t.texStorage2D(n.TEXTURE_2D,le,de,K.width,K.height)}P&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ae,Ee,J)}else t.texImage2D(n.TEXTURE_2D,0,de,ae,Ee,J);p(v)&&b(G),se.__version=ie.version,v.onUpdate&&v.onUpdate(v)}w.__version=v.version}function Ve(w,v,k){if(v.image.length!==6)return;let G=dt(w,v),q=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,w.__webglTexture,n.TEXTURE0+k);let ie=i.get(q);if(q.version!==ie.__version||G===!0){t.activeTexture(n.TEXTURE0+k);let se=rt.getPrimaries(rt.workingColorSpace),Z=v.colorSpace===Ir?null:rt.getPrimaries(v.colorSpace),J=v.colorSpace===Ir||se===Z?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,J);let ae=v.isCompressedTexture||v.image[0].isCompressedTexture,Ee=v.image[0]&&v.image[0].isDataTexture,de=[];for(let te=0;te<6;te++)!ae&&!Ee?de[te]=g(v.image[te],!0,r.maxCubemapSize):de[te]=Ee?v.image[te].image:v.image[te],de[te]=mn(v,de[te]);let ce=de[0],we=s.convert(v.format,v.colorSpace),Ne=s.convert(v.type),We=S(v.internalFormat,we,Ne,v.normalized,v.colorSpace),P=v.isVideoTexture!==!0,le=ie.__version===void 0||G===!0,K=q.dataReady,ue=E(v,ce);ot(n.TEXTURE_CUBE_MAP,v);let me;if(ae){P&&le&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ue,We,ce.width,ce.height);for(let te=0;te<6;te++){me=de[te].mipmaps;for(let Ce=0;Ce<me.length;Ce++){let be=me[Ce];v.format!==di?we!==null?P?K&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce,0,0,be.width,be.height,we,be.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce,We,be.width,be.height,0,be.data):Re("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):P?K&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce,0,0,be.width,be.height,we,Ne,be.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce,We,be.width,be.height,0,we,Ne,be.data)}}}else{if(me=v.mipmaps,P&&le){me.length>0&&ue++;let te=xt(de[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ue,We,te.width,te.height)}for(let te=0;te<6;te++)if(Ee){P?K&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,0,0,de[te].width,de[te].height,we,Ne,de[te].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,We,de[te].width,de[te].height,0,we,Ne,de[te].data);for(let Ce=0;Ce<me.length;Ce++){let At=me[Ce].image[te].image;P?K&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce+1,0,0,At.width,At.height,we,Ne,At.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce+1,We,At.width,At.height,0,we,Ne,At.data)}}else{P?K&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,0,0,we,Ne,de[te]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,We,we,Ne,de[te]);for(let Ce=0;Ce<me.length;Ce++){let be=me[Ce];P?K&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce+1,0,0,we,Ne,be.image[te]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ce+1,We,we,Ne,be.image[te])}}}p(v)&&b(n.TEXTURE_CUBE_MAP),ie.__version=q.version,v.onUpdate&&v.onUpdate(v)}w.__version=v.version}function ve(w,v,k,G,q,ie){let se=s.convert(k.format,k.colorSpace),Z=s.convert(k.type),J=S(k.internalFormat,se,Z,k.normalized,k.colorSpace),ae=i.get(v),Ee=i.get(k);if(Ee.__renderTarget=v,!ae.__hasExternalTextures){let de=Math.max(1,v.width>>ie),ce=Math.max(1,v.height>>ie);q===n.TEXTURE_3D||q===n.TEXTURE_2D_ARRAY?t.texImage3D(q,ie,J,de,ce,v.depth,0,se,Z,null):t.texImage2D(q,ie,J,de,ce,0,se,Z,null)}t.bindFramebuffer(n.FRAMEBUFFER,w),Ht(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,G,q,Ee.__webglTexture,0,Pt(v)):(q===n.TEXTURE_2D||q>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&q<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,G,q,Ee.__webglTexture,ie),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ye(w,v,k){if(n.bindRenderbuffer(n.RENDERBUFFER,w),v.depthBuffer){let G=v.depthTexture,q=G&&G.isDepthTexture?G.type:null,ie=M(v.stencilBuffer,q),se=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Ht(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Pt(v),ie,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,Pt(v),ie,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,ie,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,se,n.RENDERBUFFER,w)}else{let G=v.textures;for(let q=0;q<G.length;q++){let ie=G[q],se=s.convert(ie.format,ie.colorSpace),Z=s.convert(ie.type),J=S(ie.internalFormat,se,Z,ie.normalized,ie.colorSpace);Ht(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Pt(v),J,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,Pt(v),J,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,J,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function $t(w,v,k){let G=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,w),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("THREE.WebGLTextures: renderTarget.depthTexture must be an instance of THREE.DepthTexture.");let q=i.get(v.depthTexture);if(q.__renderTarget=v,(!q.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),G){if(q.__webglInit===void 0&&(q.__webglInit=!0,v.depthTexture.addEventListener("dispose",I)),q.__webglTexture===void 0){q.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,q.__webglTexture),ot(n.TEXTURE_CUBE_MAP,v.depthTexture);let ae=s.convert(v.depthTexture.format),Ee=s.convert(v.depthTexture.type),de;v.depthTexture.format===tr?de=n.DEPTH_COMPONENT24:v.depthTexture.format===hs&&(de=n.DEPTH24_STENCIL8);for(let ce=0;ce<6;ce++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ce,0,de,v.width,v.height,0,ae,Ee,null)}}else X(v.depthTexture,0);let ie=q.__webglTexture,se=Pt(v),Z=G?n.TEXTURE_CUBE_MAP_POSITIVE_X+k:n.TEXTURE_2D,J=v.depthTexture.format===hs?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===tr)Ht(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,J,Z,ie,0,se):n.framebufferTexture2D(n.FRAMEBUFFER,J,Z,ie,0);else if(v.depthTexture.format===hs)Ht(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,J,Z,ie,0,se):n.framebufferTexture2D(n.FRAMEBUFFER,J,Z,ie,0);else throw new Error("THREE.WebGLTextures: Unknown depthTexture format.")}function Je(w){let v=i.get(w),k=w.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==w.depthTexture){let G=w.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),G){let q=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,G.removeEventListener("dispose",q)};G.addEventListener("dispose",q),v.__depthDisposeCallback=q}v.__boundDepthTexture=G}if(w.depthTexture&&!v.__autoAllocateDepthBuffer)if(k)for(let G=0;G<6;G++)$t(v.__webglFramebuffer[G],w,G);else{let G=w.texture.mipmaps;G&&G.length>0?$t(v.__webglFramebuffer[0],w,0):$t(v.__webglFramebuffer,w,0)}else if(k){v.__webglDepthbuffer=[];for(let G=0;G<6;G++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[G]),v.__webglDepthbuffer[G]===void 0)v.__webglDepthbuffer[G]=n.createRenderbuffer(),Ye(v.__webglDepthbuffer[G],w,!1);else{let q=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ie=v.__webglDepthbuffer[G];n.bindRenderbuffer(n.RENDERBUFFER,ie),n.framebufferRenderbuffer(n.FRAMEBUFFER,q,n.RENDERBUFFER,ie)}}else{let G=w.texture.mipmaps;if(G&&G.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ye(v.__webglDepthbuffer,w,!1);else{let q=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ie=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,ie),n.framebufferRenderbuffer(n.FRAMEBUFFER,q,n.RENDERBUFFER,ie)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function ut(w,v,k){let G=i.get(w);v!==void 0&&ve(G.__webglFramebuffer,w,w.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),k!==void 0&&Je(w)}function It(w){let v=w.texture,k=i.get(w),G=i.get(v);w.addEventListener("dispose",_);let q=w.textures,ie=w.isWebGLCubeRenderTarget===!0,se=q.length>1;if(se||(G.__webglTexture===void 0&&(G.__webglTexture=n.createTexture()),G.__version=v.version,o.memory.textures++),ie){k.__webglFramebuffer=[];for(let Z=0;Z<6;Z++)if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer[Z]=[];for(let J=0;J<v.mipmaps.length;J++)k.__webglFramebuffer[Z][J]=n.createFramebuffer()}else k.__webglFramebuffer[Z]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer=[];for(let Z=0;Z<v.mipmaps.length;Z++)k.__webglFramebuffer[Z]=n.createFramebuffer()}else k.__webglFramebuffer=n.createFramebuffer();if(se)for(let Z=0,J=q.length;Z<J;Z++){let ae=i.get(q[Z]);ae.__webglTexture===void 0&&(ae.__webglTexture=n.createTexture(),o.memory.textures++)}if(w.samples>0&&Ht(w)===!1){k.__webglMultisampledFramebuffer=n.createFramebuffer(),k.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,k.__webglMultisampledFramebuffer);for(let Z=0;Z<q.length;Z++){let J=q[Z];k.__webglColorRenderbuffer[Z]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,k.__webglColorRenderbuffer[Z]);let ae=s.convert(J.format,J.colorSpace),Ee=s.convert(J.type),de=S(J.internalFormat,ae,Ee,J.normalized,J.colorSpace,w.isXRRenderTarget===!0),ce=Pt(w);n.renderbufferStorageMultisample(n.RENDERBUFFER,ce,de,w.width,w.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Z,n.RENDERBUFFER,k.__webglColorRenderbuffer[Z])}n.bindRenderbuffer(n.RENDERBUFFER,null),w.depthBuffer&&(k.__webglDepthRenderbuffer=n.createRenderbuffer(),Ye(k.__webglDepthRenderbuffer,w,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(ie){t.bindTexture(n.TEXTURE_CUBE_MAP,G.__webglTexture),ot(n.TEXTURE_CUBE_MAP,v);for(let Z=0;Z<6;Z++)if(v.mipmaps&&v.mipmaps.length>0)for(let J=0;J<v.mipmaps.length;J++)ve(k.__webglFramebuffer[Z][J],w,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,J);else ve(k.__webglFramebuffer[Z],w,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0);p(v)&&b(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(se){for(let Z=0,J=q.length;Z<J;Z++){let ae=q[Z],Ee=i.get(ae),de=n.TEXTURE_2D;(w.isWebGL3DRenderTarget||w.isWebGLArrayRenderTarget)&&(de=w.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(de,Ee.__webglTexture),ot(de,ae),ve(k.__webglFramebuffer,w,ae,n.COLOR_ATTACHMENT0+Z,de,0),p(ae)&&b(de)}t.unbindTexture()}else{let Z=n.TEXTURE_2D;if((w.isWebGL3DRenderTarget||w.isWebGLArrayRenderTarget)&&(Z=w.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(Z,G.__webglTexture),ot(Z,v),v.mipmaps&&v.mipmaps.length>0)for(let J=0;J<v.mipmaps.length;J++)ve(k.__webglFramebuffer[J],w,v,n.COLOR_ATTACHMENT0,Z,J);else ve(k.__webglFramebuffer,w,v,n.COLOR_ATTACHMENT0,Z,0);p(v)&&b(Z),t.unbindTexture()}w.depthBuffer&&Je(w)}function et(w){let v=w.textures;for(let k=0,G=v.length;k<G;k++){let q=v[k];if(p(q)){let ie=A(w),se=i.get(q).__webglTexture;t.bindTexture(ie,se),b(ie),t.unbindTexture()}}}let Nt=[],en=[];function Ln(w){if(w.samples>0){if(Ht(w)===!1){let v=w.textures,k=w.width,G=w.height,q=n.COLOR_BUFFER_BIT,ie=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,se=i.get(w),Z=v.length>1;if(Z)for(let ae=0;ae<v.length;ae++)t.bindFramebuffer(n.FRAMEBUFFER,se.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ae,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,se.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ae,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,se.__webglMultisampledFramebuffer);let J=w.texture.mipmaps;J&&J.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,se.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,se.__webglFramebuffer);for(let ae=0;ae<v.length;ae++){if(w.resolveDepthBuffer&&(w.depthBuffer&&(q|=n.DEPTH_BUFFER_BIT),w.stencilBuffer&&w.resolveStencilBuffer&&(q|=n.STENCIL_BUFFER_BIT)),Z){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,se.__webglColorRenderbuffer[ae]);let Ee=i.get(v[ae]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Ee,0)}n.blitFramebuffer(0,0,k,G,0,0,k,G,q,n.NEAREST),c===!0&&(Nt.length=0,en.length=0,Nt.push(n.COLOR_ATTACHMENT0+ae),w.depthBuffer&&w.storeMultisampledDepthBuffer===!1&&(Nt.push(ie),en.push(ie),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,en)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Nt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),Z)for(let ae=0;ae<v.length;ae++){t.bindFramebuffer(n.FRAMEBUFFER,se.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ae,n.RENDERBUFFER,se.__webglColorRenderbuffer[ae]);let Ee=i.get(v[ae]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,se.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ae,n.TEXTURE_2D,Ee,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,se.__webglMultisampledFramebuffer)}else if(w.depthBuffer&&w.storeMultisampledDepthBuffer===!1&&c){let v=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function Pt(w){return Math.min(r.maxSamples,w.samples)}function Ht(w){let v=i.get(w);return w.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function L(w){let v=o.render.frame;u.get(w)!==v&&(u.set(w,v),w.update())}function mn(w,v){let k=w.colorSpace,G=w.format,q=w.type;return w.isCompressedTexture===!0||w.isVideoTexture===!0||k!==dl&&k!==Ir&&(rt.getTransfer(k)===_t?(G!==di||q!==ei)&&Re("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Le("WebGLTextures: Unsupported texture color space:",k)),v}function xt(w){return typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement?(l.width=w.naturalWidth||w.width,l.height=w.naturalHeight||w.height):typeof VideoFrame<"u"&&w instanceof VideoFrame?(l.width=w.displayWidth,l.height=w.displayHeight):(l.width=w.width,l.height=w.height),l}this.allocateTextureUnit=$,this.resetTextureUnits=V,this.getTextureUnits=D,this.setTextureUnits=U,this.setTexture2D=X,this.setTexture2DArray=W,this.setTexture3D=Q,this.setTextureCube=ne,this.rebindTextures=ut,this.setupRenderTarget=It,this.updateRenderTargetMipmap=et,this.updateMultisampleRenderTarget=Ln,this.setupDepthRenderbuffer=Je,this.setupFrameBufferTexture=ve,this.useMultisampledRTT=Ht,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function QU(n,e){function t(i,r=Ir){let s,o=rt.getTransfer(r);if(i===ei)return n.UNSIGNED_BYTE;if(i===Hh)return n.UNSIGNED_SHORT_4_4_4_4;if(i===zh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Xy)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Yy)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===$y)return n.BYTE;if(i===qy)return n.SHORT;if(i===Ra)return n.UNSIGNED_SHORT;if(i===Vh)return n.INT;if(i===Oi)return n.UNSIGNED_INT;if(i===Fi)return n.FLOAT;if(i===Ui)return n.HALF_FLOAT;if(i===Zy)return n.ALPHA;if(i===Ky)return n.RGB;if(i===di)return n.RGBA;if(i===tr)return n.DEPTH_COMPONENT;if(i===hs)return n.DEPTH_STENCIL;if(i===Jy)return n.RED;if(i===Gh)return n.RED_INTEGER;if(i===ps)return n.RG;if(i===Wh)return n.RG_INTEGER;if(i===jh)return n.RGBA_INTEGER;if(i===Ul||i===kl||i===Bl||i===Vl)if(o===_t)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Ul)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===kl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Bl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Vl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Ul)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===kl)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Bl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Vl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===$h||i===qh||i===Xh||i===Yh)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===$h)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===qh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Xh)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Yh)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Zh||i===Kh||i===Jh||i===Qh||i===ep||i===Hl||i===tp)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Zh||i===Kh)return o===_t?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Jh)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Qh)return s.COMPRESSED_R11_EAC;if(i===ep)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Hl)return s.COMPRESSED_RG11_EAC;if(i===tp)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===np||i===ip||i===rp||i===sp||i===op||i===ap||i===cp||i===lp||i===up||i===dp||i===fp||i===hp||i===pp||i===mp)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===np)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===ip)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===rp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===sp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===op)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===ap)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===cp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===lp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===up)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===dp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===fp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===hp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===pp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===mp)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===gp||i===vp||i===yp)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===gp)return o===_t?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===vp)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===yp)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===_p||i===xp||i===zl||i===Sp)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===_p)return s.COMPRESSED_RED_RGTC1_EXT;if(i===xp)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===zl)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Sp)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Na?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var ek=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,tk=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,S_=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Cl(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Kn({vertexShader:ek,fragmentShader:tk,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new En(new no(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},b_=class extends Pi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,m=null,y=typeof XRWebGLBinding<"u",g=new S_,p={},b=t.getContextAttributes(),A=null,S=null,M=[],E=[],I=new Pe,_=null,T=null,R=new Mn;R.viewport=new Ot;let O=new Mn;O.viewport=new Ot;let C=[R,O],V=new Fh,D=null,U=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let ee=M[Y];return ee===void 0&&(ee=new wa,M[Y]=ee),ee.getTargetRaySpace()},this.getControllerGrip=function(Y){let ee=M[Y];return ee===void 0&&(ee=new wa,M[Y]=ee),ee.getGripSpace()},this.getHand=function(Y){let ee=M[Y];return ee===void 0&&(ee=new wa,M[Y]=ee),ee.getHandSpace()};function $(Y){let ee=E.indexOf(Y.inputSource);if(ee===-1)return;let _e=M[ee];_e!==void 0&&(_e.update(Y.inputSource,Y.frame,l||o),_e.dispatchEvent({type:Y.type,data:Y.inputSource}))}function B(){r.removeEventListener("select",$),r.removeEventListener("selectstart",$),r.removeEventListener("selectend",$),r.removeEventListener("squeeze",$),r.removeEventListener("squeezestart",$),r.removeEventListener("squeezeend",$),r.removeEventListener("end",B),r.removeEventListener("inputsourceschange",X);for(let Y=0;Y<M.length;Y++){let ee=E[Y];ee!==null&&(E[Y]=null,M[Y].disconnect(ee))}D=null,U=null,g.reset();for(let Y in p)delete p[Y];if(e.setRenderTarget(A),h=null,f=null,d=null,r=null,S=null,dt.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(I.width,I.height,!1),T!==null){let Y=T.camera;Y.fov=T.fov,Y.zoom=T.zoom,Y.updateProjectionMatrix(),T=null}i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&Re("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&Re("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return m},this.getSession=function(){return r},this.setSession=async function(Y){if(r=Y,r!==null){if(A=e.getRenderTarget(),r.addEventListener("select",$),r.addEventListener("selectstart",$),r.addEventListener("selectend",$),r.addEventListener("squeeze",$),r.addEventListener("squeezestart",$),r.addEventListener("squeezeend",$),r.addEventListener("end",B),r.addEventListener("inputsourceschange",X),b.xrCompatible!==!0&&await t.makeXRCompatible(),_=e.getPixelRatio(),e.getSize(I),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let _e=null,Ve=null,ve=null;b.depth&&(ve=b.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,_e=b.stencil?hs:tr,Ve=b.stencil?Na:Oi);let Ye={colorFormat:t.RGBA8,depthFormat:ve,scaleFactor:s};d=this.getBinding(),f=d.createProjectionLayer(Ye),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),S=new Un(f.textureWidth,f.textureHeight,{format:di,type:ei,depthTexture:new is(f.textureWidth,f.textureHeight,Ve,void 0,void 0,void 0,void 0,void 0,void 0,_e),stencilBuffer:b.stencil,colorSpace:e.outputColorSpace,samples:b.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1,storeMultisampledDepthBuffer:f.ignoreDepthValues===!1,storeMultisampledStencilBuffer:f.ignoreDepthValues===!1})}else{let _e={antialias:b.antialias,alpha:!0,depth:b.depth,stencil:b.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,_e),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new Un(h.framebufferWidth,h.framebufferHeight,{format:di,type:ei,colorSpace:e.outputColorSpace,stencilBuffer:b.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1,storeMultisampledDepthBuffer:h.ignoreDepthValues===!1,storeMultisampledStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),dt.setContext(r),dt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return g.getDepthTexture()};function X(Y){for(let ee=0;ee<Y.removed.length;ee++){let _e=Y.removed[ee],Ve=E.indexOf(_e);Ve>=0&&(E[Ve]=null,M[Ve].disconnect(_e))}for(let ee=0;ee<Y.added.length;ee++){let _e=Y.added[ee],Ve=E.indexOf(_e);if(Ve===-1){for(let Ye=0;Ye<M.length;Ye++)if(Ye>=E.length){E.push(_e),Ve=Ye;break}else if(E[Ye]===null){E[Ye]=_e,Ve=Ye;break}if(Ve===-1)break}let ve=M[Ve];ve&&ve.connect(_e)}}let W=new F,Q=new F;function ne(Y,ee,_e){W.setFromMatrixPosition(ee.matrixWorld),Q.setFromMatrixPosition(_e.matrixWorld);let Ve=W.distanceTo(Q),ve=ee.projectionMatrix.elements,Ye=_e.projectionMatrix.elements,$t=ve[14]/(ve[10]-1),Je=ve[14]/(ve[10]+1),ut=(ve[9]+1)/ve[5],It=(ve[9]-1)/ve[5],et=(ve[8]-1)/ve[0],Nt=(Ye[8]+1)/Ye[0],en=$t*et,Ln=$t*Nt,Pt=Ve/(-et+Nt),Ht=Pt*-et;if(ee.matrixWorld.decompose(Y.position,Y.quaternion,Y.scale),Y.translateX(Ht),Y.translateZ(Pt),Y.matrixWorld.compose(Y.position,Y.quaternion,Y.scale),Y.matrixWorldInverse.copy(Y.matrixWorld).invert(),ve[10]===-1)Y.projectionMatrix.copy(ee.projectionMatrix),Y.projectionMatrixInverse.copy(ee.projectionMatrixInverse);else{let L=$t+Pt,mn=Je+Pt,xt=en-Ht,w=Ln+(Ve-Ht),v=ut*Je/mn*L,k=It*Je/mn*L;Y.projectionMatrix.makePerspective(xt,w,v,k,L,mn),Y.projectionMatrixInverse.copy(Y.projectionMatrix).invert()}}function Ie(Y,ee){ee===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(ee.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let ee=Y.near,_e=Y.far;g.texture!==null&&(g.depthNear>0&&(ee=g.depthNear),g.depthFar>0&&(_e=g.depthFar)),V.near=O.near=R.near=ee,V.far=O.far=R.far=_e,(D!==V.near||U!==V.far)&&(r.updateRenderState({depthNear:V.near,depthFar:V.far}),D=V.near,U=V.far),V.layers.mask=Y.layers.mask|6,R.layers.mask=V.layers.mask&-5,O.layers.mask=V.layers.mask&-3;let Ve=Y.parent,ve=V.cameras;Ie(V,Ve);for(let Ye=0;Ye<ve.length;Ye++)Ie(ve[Ye],Ve);ve.length===2?ne(V,R,O):V.projectionMatrix.copy(R.projectionMatrix),T===null&&Y.isPerspectiveCamera&&(T={camera:Y,fov:Y.fov,zoom:Y.zoom}),Te(Y,V,Ve)};function Te(Y,ee,_e){_e===null?Y.matrix.copy(ee.matrixWorld):(Y.matrix.copy(_e.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(ee.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(ee.projectionMatrix),Y.projectionMatrixInverse.copy(ee.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=Ma*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return V},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(Y){c=Y,f!==null&&(f.fixedFoveation=Y),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=Y)},this.hasDepthSensing=function(){return g.texture!==null},this.getDepthSensingMesh=function(){return g.getMesh(V)},this.getCameraTexture=function(Y){return p[Y]};let Ct=null;function ot(Y,ee){if(u=ee.getViewerPose(l||o),m=ee,u!==null){let _e=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Ve=!1;_e.length!==V.cameras.length&&(V.cameras.length=0,Ve=!0);for(let Je=0;Je<_e.length;Je++){let ut=_e[Je],It=null;if(h!==null)It=h.getViewport(ut);else{let Nt=d.getViewSubImage(f,ut);It=Nt.viewport,Je===0&&(e.setRenderTargetTextures(S,Nt.colorTexture,Nt.depthStencilTexture),e.setRenderTarget(S))}let et=C[Je];et===void 0&&(et=new Mn,et.layers.enable(Je),et.viewport=new Ot,C[Je]=et),et.matrix.fromArray(ut.transform.matrix),et.matrix.decompose(et.position,et.quaternion,et.scale),et.projectionMatrix.fromArray(ut.projectionMatrix),et.projectionMatrixInverse.copy(et.projectionMatrix).invert(),et.viewport.set(It.x,It.y,It.width,It.height),Je===0&&(V.matrix.copy(et.matrix),V.matrix.decompose(V.position,V.quaternion,V.scale)),Ve===!0&&V.cameras.push(et)}let ve=r.enabledFeatures;if(ve&&ve.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let Je=d.getDepthInformation(_e[0]);Je&&Je.isValid&&Je.texture&&g.init(Je,r.renderState)}if(ve&&ve.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let Je=0;Je<_e.length;Je++){let ut=_e[Je].camera;if(ut){let It=p[ut];It||(It=new Cl,p[ut]=It);let et=d.getCameraImage(ut);It.sourceTexture=et}}}}for(let _e=0;_e<M.length;_e++){let Ve=E[_e],ve=M[_e];Ve!==null&&ve!==void 0&&ve.update(Ve,ee,l||o)}Ct&&Ct(Y,ee),ee.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ee}),m=null}let dt=new ww;dt.setAnimationLoop(ot),this.setAnimationLoop=function(Y){Ct=Y},this.dispose=function(){}}},nk=new Lt,Rw=new Be;Rw.set(-1,0,0,0,1,0,0,0,1);function ik(n,e){function t(g,p){g.matrixAutoUpdate===!0&&g.updateMatrix(),p.value.copy(g.matrix)}function i(g,p){p.color.getRGB(g.fogColor.value,r_(n)),p.isFog?(g.fogNear.value=p.near,g.fogFar.value=p.far):p.isFogExp2&&(g.fogDensity.value=p.density)}function r(g,p,b,A,S){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(g,p):p.isMeshLambertMaterial?(s(g,p),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(g,p),d(g,p)):p.isMeshPhongMaterial?(s(g,p),u(g,p),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(g,p),f(g,p),p.isMeshPhysicalMaterial&&h(g,p,S)):p.isMeshMatcapMaterial?(s(g,p),m(g,p)):p.isMeshDepthMaterial?s(g,p):p.isMeshDistanceMaterial?(s(g,p),y(g,p)):p.isMeshNormalMaterial?s(g,p):p.isLineBasicMaterial?(o(g,p),p.isLineDashedMaterial&&a(g,p)):p.isPointsMaterial?c(g,p,b,A):p.isSpriteMaterial?l(g,p):p.isShadowMaterial?(g.color.value.copy(p.color),g.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(g,p){g.opacity.value=p.opacity,p.color&&g.diffuse.value.copy(p.color),p.emissive&&g.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.bumpMap&&(g.bumpMap.value=p.bumpMap,t(p.bumpMap,g.bumpMapTransform),g.bumpScale.value=p.bumpScale,p.side===Pn&&(g.bumpScale.value*=-1)),p.normalMap&&(g.normalMap.value=p.normalMap,t(p.normalMap,g.normalMapTransform),g.normalScale.value.copy(p.normalScale),p.side===Pn&&g.normalScale.value.negate()),p.displacementMap&&(g.displacementMap.value=p.displacementMap,t(p.displacementMap,g.displacementMapTransform),g.displacementScale.value=p.displacementScale,g.displacementBias.value=p.displacementBias),p.emissiveMap&&(g.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,g.emissiveMapTransform)),p.specularMap&&(g.specularMap.value=p.specularMap,t(p.specularMap,g.specularMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest);let b=e.get(p),A=b.envMap,S=b.envMapRotation;A&&(g.envMap.value=A,g.envMapRotation.value.setFromMatrix4(nk.makeRotationFromEuler(S)).transpose(),A.isCubeTexture&&A.isRenderTargetTexture===!1&&g.envMapRotation.value.premultiply(Rw),g.reflectivity.value=p.reflectivity,g.ior.value=p.ior,g.refractionRatio.value=p.refractionRatio),p.lightMap&&(g.lightMap.value=p.lightMap,g.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,g.lightMapTransform)),p.aoMap&&(g.aoMap.value=p.aoMap,g.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,g.aoMapTransform))}function o(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform))}function a(g,p){g.dashSize.value=p.dashSize,g.totalSize.value=p.dashSize+p.gapSize,g.scale.value=p.scale}function c(g,p,b,A){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.size.value=p.size*b,g.scale.value=A*.5,p.map&&(g.map.value=p.map,t(p.map,g.uvTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function l(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.rotation.value=p.rotation,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function u(g,p){g.specular.value.copy(p.specular),g.shininess.value=Math.max(p.shininess,1e-4)}function d(g,p){p.gradientMap&&(g.gradientMap.value=p.gradientMap)}function f(g,p){g.metalness.value=p.metalness,p.metalnessMap&&(g.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,g.metalnessMapTransform)),g.roughness.value=p.roughness,p.roughnessMap&&(g.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,g.roughnessMapTransform)),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)}function h(g,p,b){g.ior.value=p.ior,p.sheen>0&&(g.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),g.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(g.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,g.sheenColorMapTransform)),p.sheenRoughnessMap&&(g.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,g.sheenRoughnessMapTransform))),p.clearcoat>0&&(g.clearcoat.value=p.clearcoat,g.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(g.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,g.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(g.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,g.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(g.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,g.clearcoatNormalMapTransform),g.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===Pn&&g.clearcoatNormalScale.value.negate())),p.dispersion>0&&(g.dispersion.value=p.dispersion),p.retroreflectivity>0&&(g.retroreflectivity.value=p.retroreflectivity),p.iridescence>0&&(g.iridescence.value=p.iridescence,g.iridescenceIOR.value=p.iridescenceIOR,g.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],g.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(g.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,g.iridescenceMapTransform)),p.iridescenceThicknessMap&&(g.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,g.iridescenceThicknessMapTransform))),p.transmission>0&&(g.transmission.value=p.transmission,g.transmissionSamplerMap.value=b.texture,g.transmissionSamplerSize.value.set(b.width,b.height),p.transmissionMap&&(g.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,g.transmissionMapTransform)),g.thickness.value=p.thickness,p.thicknessMap&&(g.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,g.thicknessMapTransform)),g.attenuationDistance.value=p.attenuationDistance,g.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(g.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(g.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,g.anisotropyMapTransform))),g.specularIntensity.value=p.specularIntensity,g.specularColor.value.copy(p.specularColor),p.specularColorMap&&(g.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,g.specularColorMapTransform)),p.specularIntensityMap&&(g.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,g.specularIntensityMapTransform))}function m(g,p){p.matcap&&(g.matcap.value=p.matcap)}function y(g,p){let b=e.get(p).light;g.referencePosition.value.setFromMatrixPosition(b.matrixWorld),g.nearDistance.value=b.shadow.camera.near,g.farDistance.value=b.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function rk(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(S,M){let E=M.program;i.uniformBlockBinding(S,E)}function l(S,M){let E=r[S.id];E===void 0&&(g(S),E=u(S),r[S.id]=E,S.addEventListener("dispose",b));let I=M.program;i.updateUBOMapping(S,I);let _=e.render.frame;s[S.id]!==_&&(f(S),s[S.id]=_)}function u(S){let M=d();S.__bindingPointIndex=M;let E=n.createBuffer(),I=S.__size,_=S.usage;return n.bindBuffer(n.UNIFORM_BUFFER,E),n.bufferData(n.UNIFORM_BUFFER,I,_),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,M,E),E}function d(){for(let S=0;S<a;S++)if(o.indexOf(S)===-1)return o.push(S),S;return Le("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(S){let M=r[S.id],E=S.uniforms,I=S.__cache;n.bindBuffer(n.UNIFORM_BUFFER,M);for(let _=0,T=E.length;_<T;_++){let R=E[_];if(Array.isArray(R))for(let O=0,C=R.length;O<C;O++)h(R[O],_,O,I);else h(R,_,0,I)}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(S,M,E,I){if(y(S,M,E,I)===!0){let _=S.__offset,T=S.value;if(Array.isArray(T)){let R=0;for(let O=0;O<T.length;O++){let C=T[O],V=p(C);m(C,S.__data,R),typeof C!="number"&&typeof C!="boolean"&&!C.isMatrix3&&!ArrayBuffer.isView(C)&&(R+=V.storage/Float32Array.BYTES_PER_ELEMENT)}}else m(T,S.__data,0);n.bufferSubData(n.UNIFORM_BUFFER,_,S.__data)}}function m(S,M,E){typeof S=="number"||typeof S=="boolean"?M[0]=S:S.isMatrix3?(M[0]=S.elements[0],M[1]=S.elements[1],M[2]=S.elements[2],M[3]=0,M[4]=S.elements[3],M[5]=S.elements[4],M[6]=S.elements[5],M[7]=0,M[8]=S.elements[6],M[9]=S.elements[7],M[10]=S.elements[8],M[11]=0):ArrayBuffer.isView(S)?M.set(new S.constructor(S.buffer,S.byteOffset,M.length)):S.toArray(M,E)}function y(S,M,E,I){let _=S.value,T=M+"_"+E;if(I[T]===void 0)return typeof _=="number"||typeof _=="boolean"?I[T]=_:ArrayBuffer.isView(_)?I[T]=_.slice():I[T]=_.clone(),!0;{let R=I[T];if(typeof _=="number"||typeof _=="boolean"){if(R!==_)return I[T]=_,!0}else{if(ArrayBuffer.isView(_))return!0;if(R.equals(_)===!1)return R.copy(_),!0}}return!1}function g(S){let M=S.uniforms,E=0,I=16;for(let T=0,R=M.length;T<R;T++){let O=Array.isArray(M[T])?M[T]:[M[T]];for(let C=0,V=O.length;C<V;C++){let D=O[C],U=Array.isArray(D.value)?D.value:[D.value];for(let $=0,B=U.length;$<B;$++){let X=U[$],W=p(X),Q=E%I,ne=Q%W.boundary,Ie=Q+ne;E+=ne,Ie!==0&&I-Ie<W.storage&&(E+=I-Ie),D.__data=new Float32Array(W.storage/Float32Array.BYTES_PER_ELEMENT),D.__offset=E,E+=W.storage}}}let _=E%I;return _>0&&(E+=I-_),S.__size=E,S.__cache={},this}function p(S){let M={boundary:0,storage:0};return typeof S=="number"||typeof S=="boolean"?(M.boundary=4,M.storage=4):S.isVector2?(M.boundary=8,M.storage=8):S.isVector3||S.isColor?(M.boundary=16,M.storage=12):S.isVector4?(M.boundary=16,M.storage=16):S.isMatrix3?(M.boundary=48,M.storage=48):S.isMatrix4?(M.boundary=64,M.storage=64):S.isTexture?Re("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(S)?(M.boundary=16,M.storage=S.byteLength):Re("WebGLRenderer: Unsupported uniform value type.",S),M}function b(S){let M=S.target;M.removeEventListener("dispose",b);let E=o.indexOf(M.__bindingPointIndex);o.splice(E,1),n.deleteBuffer(r[M.id]),delete r[M.id],delete s[M.id]}function A(){for(let S in r)n.deleteBuffer(r[S]);o=[],r={},s={}}return{bind:c,update:l,dispose:A}}var sk=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),rr=null;function ok(){return rr===null&&(rr=new _h(sk,16,16,ps,Ui),rr.name="DFG_LUT",rr.minFilter=hn,rr.magFilter=hn,rr.wrapS=er,rr.wrapT=er,rr.generateMipmaps=!1,rr.needsUpdate=!0),rr}var Ip=class{constructor(e={}){let{canvas:t=ZE(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=ei}=e;this.isWebGLRenderer=!0;let m;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");m=i.getContextAttributes().alpha}else m=o;let y=h,g=new Set([jh,Wh,Gh]),p=new Set([ei,Oi,Ra,Na,Hh,zh]),b=new Uint32Array(4),A=new Int32Array(4),S=new F,M=null,E=null,I=[],_=[],T=null;this.domElement=t,this.debug={checkShaderErrors:!0,diagnostics:{keywords:!1},onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=Li,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let R=this,O=!1,C=null,V=null,D=null,U=null;this._outputColorSpace=Xn;let $=0,B=0,X=null,W=-1,Q=null,ne=new Ot,Ie=new Ot,Te=null,Ct=new st(0),ot=0,dt=t.width,Y=t.height,ee=1,_e=null,Ve=null,ve=new Ot(0,0,dt,Y),Ye=new Ot(0,0,dt,Y),$t=!1,Je=new El,ut=!1,It=!1,et=new Lt,Nt=new F,en=new Ot,Ln={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Pt=!1;function Ht(){return X===null?ee:1}let L=i;function mn(x,N){return t.getContext(x,N)}let xt,w,v,k,G,q,ie,se,Z,J,ae,Ee,de,ce,we,Ne,We,P,le,K,ue,me,te;try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"186"}`),t.addEventListener("webglcontextlost",At,!1),t.addEventListener("webglcontextrestored",mt,!1),t.addEventListener("webglcontextcreationerror",fi,!1),L===null){let N="webgl2";if(L=mn(N,x),L===null)throw mn(N)?new Error("THREE.WebGLRenderer: Error creating WebGL context with your selected attributes."):new Error("THREE.WebGLRenderer: Error creating WebGL context.")}Ce()}catch(x){throw t.removeEventListener("webglcontextlost",At,!1),t.removeEventListener("webglcontextrestored",mt,!1),t.removeEventListener("webglcontextcreationerror",fi,!1),Le("WebGLRenderer: "+x.message),x}function Ce(){xt=new hF(L),xt.init(),ue=new QU(L,xt),w=new iF(L,xt,e,ue),v=new KU(L,xt),w.reversedDepthBuffer&&f&&v.buffers.depth.setReversed(!0),V=L.createFramebuffer(),D=L.createFramebuffer(),U=L.createFramebuffer(),k=new gF(L),G=new UU,q=new JU(L,xt,v,G,w,ue,k),ie=new fF(R),se=new yP(L),me=new tF(L,se),Z=new pF(L,se,k,me),J=new yF(L,Z,se,me,k),P=new vF(L,w,q),we=new rF(G),ae=new FU(R,ie,xt,w,me,we),Ee=new ik(R,G),de=new BU,ce=new jU(xt),We=new eF(R,ie,v,J,m,c),Ne=new ZU(R,J,w),te=new rk(L,k,w,v),le=new nF(L,xt,k),K=new mF(L,xt,k),k.programs=ae.programs,R.capabilities=w,R.extensions=xt,R.properties=G,R.renderLists=de,R.shadowMap=Ne,R.state=v,R.info=k}y!==ei&&(T=new xF(y,t.width,t.height,a,r,s));let be=new b_(R,L);this.xr=be,this.getContext=function(){return L},this.getContextAttributes=function(){return L.getContextAttributes()},this.forceContextLoss=function(){let x=xt.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=xt.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return ee},this.setPixelRatio=function(x){x!==void 0&&(ee=x,this.setSize(dt,Y,!1))},this.getSize=function(x){return x.set(dt,Y)},this.setSize=function(x,N,j=!0){if(be.isPresenting){Re("WebGLRenderer: Can't change size while VR device is presenting.");return}dt=x,Y=N,t.width=Math.floor(x*ee),t.height=Math.floor(N*ee),j===!0&&(t.style.width=x+"px",t.style.height=N+"px"),T!==null&&T.setSize(t.width,t.height),this.setViewport(0,0,x,N)},this.getDrawingBufferSize=function(x){return x.set(dt*ee,Y*ee).floor()},this.setDrawingBufferSize=function(x,N,j){dt=x,Y=N,ee=j,t.width=Math.floor(x*j),t.height=Math.floor(N*j),this.setViewport(0,0,x,N)},this.setEffects=function(x){if(y===ei){Le("WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let N=0;N<x.length;N++)if(x[N].isOutputPass===!0){Re("WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}T.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(ne)},this.getViewport=function(x){return x.copy(ve)},this.setViewport=function(x,N,j,H){x.isVector4?ve.set(x.x,x.y,x.z,x.w):ve.set(x,N,j,H),v.viewport(ne.copy(ve).multiplyScalar(ee).round())},this.getScissor=function(x){return x.copy(Ye)},this.setScissor=function(x,N,j,H){x.isVector4?Ye.set(x.x,x.y,x.z,x.w):Ye.set(x,N,j,H),v.scissor(Ie.copy(Ye).multiplyScalar(ee).round())},this.getScissorTest=function(){return $t},this.setScissorTest=function(x){v.setScissorTest($t=x)},this.setOpaqueSort=function(x){_e=x},this.setTransparentSort=function(x){Ve=x},this.getClearColor=function(x){return x.copy(We.getClearColor())},this.setClearColor=function(){We.setClearColor(...arguments)},this.getClearAlpha=function(){return We.getClearAlpha()},this.setClearAlpha=function(){We.setClearAlpha(...arguments)},this.clear=function(x=!0,N=!0,j=!0){let H=0;if(x){let z=!1;if(X!==null){let pe=X.texture.format;z=g.has(pe)}if(z){let pe=X.texture.type,ye=p.has(pe),he=We.getClearColor(),xe=We.getClearAlpha(),Me=he.r,$e=he.g,Qe=he.b;ye?(b[0]=Me,b[1]=$e,b[2]=Qe,b[3]=xe,L.clearBufferuiv(L.COLOR,0,b)):(A[0]=Me,A[1]=$e,A[2]=Qe,A[3]=xe,L.clearBufferiv(L.COLOR,0,A))}else H|=L.COLOR_BUFFER_BIT}N&&(H|=L.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),j&&(H|=L.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),H!==0&&L.clear(H)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(x){x.setRenderer(this),C=x},this.dispose=function(){t.removeEventListener("webglcontextlost",At,!1),t.removeEventListener("webglcontextrestored",mt,!1),t.removeEventListener("webglcontextcreationerror",fi,!1),We.dispose(),de.dispose(),ce.dispose(),G.dispose(),ie.dispose(),J.dispose(),me.dispose(),te.dispose(),ae.dispose(),be.dispose(),be.removeEventListener("sessionstart",A_),be.removeEventListener("sessionend",D_),ms.stop()};function At(x){x.preventDefault(),ml("WebGLRenderer: Context Lost."),O=!0}function mt(){ml("WebGLRenderer: Context Restored."),O=!1;let x=k.autoReset,N=Ne.enabled,j=Ne.autoUpdate,H=Ne.needsUpdate,z=Ne.type;Ce(),k.autoReset=x,Ne.enabled=N,Ne.autoUpdate=j,Ne.needsUpdate=H,Ne.type=z}function fi(x){Le("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function ki(x){let N=x.target;N.removeEventListener("dispose",ki),Vw(N)}function Vw(x){Hw(x),G.remove(x)}function Hw(x){let N=G.get(x).programs;N!==void 0&&(N.forEach(function(j){ae.releaseProgram(j)}),x.isShaderMaterial&&ae.releaseShaderCache(x))}this.renderBufferDirect=function(x,N,j,H,z,pe){N===null&&(N=Ln);let ye=z.isMesh&&z.matrixWorld.determinantAffine()<0,he=Ww(x,N,j,H,z);v.setMaterial(H,ye);let xe=j.index,Me=1;if(H.wireframe===!0){if(xe=Z.getWireframeAttribute(j),xe===void 0)return;Me=2}let $e=j.drawRange,Qe=j.attributes.position,Se=$e.start*Me,gt=($e.start+$e.count)*Me;pe!==null&&(Se=Math.max(Se,pe.start*Me),gt=Math.min(gt,(pe.start+pe.count)*Me)),xe!==null?(Se=Math.max(Se,0),gt=Math.min(gt,xe.count)):Qe!=null&&(Se=Math.max(Se,0),gt=Math.min(gt,Qe.count));let zt=gt-Se;if(zt<0||zt===1/0)return;me.setup(z,H,he,j,xe);let Rt,Tt=le;if(xe!==null&&(Rt=se.get(xe),Tt=K,Tt.setIndex(Rt)),z.isMesh)H.wireframe===!0?(v.setLineWidth(H.wireframeLinewidth*Ht()),Tt.setMode(L.LINES)):Tt.setMode(L.TRIANGLES);else if(z.isLine){let gn=H.linewidth;gn===void 0&&(gn=1),v.setLineWidth(gn*Ht()),z.isLineSegments?Tt.setMode(L.LINES):z.isLineLoop?Tt.setMode(L.LINE_LOOP):Tt.setMode(L.LINE_STRIP)}else z.isPoints?Tt.setMode(L.POINTS):z.isSprite&&Tt.setMode(L.TRIANGLES);if(z.isBatchedMesh)if(xt.get("WEBGL_multi_draw"))Tt.renderMultiDraw(z._multiDrawStarts,z._multiDrawCounts,z._multiDrawCount);else{let gn=z._multiDrawStarts,ge=z._multiDrawCounts,Tn=z._multiDrawCount,ct=xe?se.get(xe).bytesPerElement:1,ti=G.get(H).currentProgram.getUniforms();for(let Bi=0;Bi<Tn;Bi++)ti.setValue(L,"_gl_DrawID",Bi),Tt.render(gn[Bi]/ct,ge[Bi])}else if(z.isInstancedMesh)Tt.renderInstances(Se,zt,z.count);else if(j.isInstancedBufferGeometry){let gn=j._maxInstanceCount!==void 0?j._maxInstanceCount:1/0,ge=Math.min(j.instanceCount,gn);Tt.renderInstances(Se,zt,ge)}else Tt.render(Se,zt)};function I_(x,N,j,H){C!==null&&x.isNodeMaterial&&C.setObject(H,x),ut===!0&&we.setState(x,j,!1),x.transparent===!0&&x.side===Qn&&x.forceSinglePass===!1?(x.side=Pn,x.needsUpdate=!0,Xl(x,N,H),x.side=us,x.needsUpdate=!0,Xl(x,N,H),x.side=Qn):Xl(x,N,H)}this.compile=function(x,N,j=null){j===null&&(j=x),C!==null&&C.renderStart(x,N,j),E=ce.get(j),E.init(N),_.push(E),j.traverseVisible(function(z){z.isLight&&z.layers.test(N.layers)&&(E.pushLight(z),z.castShadow&&E.pushShadow(z))}),x!==j&&x.traverseVisible(function(z){z.isLight&&z.layers.test(N.layers)&&(E.pushLight(z),z.castShadow&&E.pushShadow(z))}),E.setupLights(),C!==null&&C.updateLights(E.state.lightsArray),It=this.localClippingEnabled,ut=we.init(this.clippingPlanes,It),ut===!0&&we.setGlobalState(this.clippingPlanes,N),C!==null&&Ne.render(E.state.shadowsArray,j,N);let H=new Set;return x.traverse(function(z){if(!(z.isMesh||z.isPoints||z.isLine||z.isSprite))return;let pe=z.material;if(pe)if(Array.isArray(pe))for(let ye=0;ye<pe.length;ye++){let he=pe[ye];I_(he,j,N,z),H.add(he)}else I_(pe,j,N,z),H.add(pe)}),E=_.pop(),C!==null&&C.renderEnd(),H},this.compileAsync=function(x,N,j=null){let H=this.compile(x,N,j);return new Promise(z=>{function pe(){if(H.forEach(function(ye){let xe=G.get(ye).currentProgram;(xe===void 0||xe.isReady())&&H.delete(ye)}),H.size===0){z(x);return}setTimeout(pe,10)}xt.get("KHR_parallel_shader_compile")!==null?pe():setTimeout(pe,10)})};let Lp=null;function zw(x){Lp&&Lp(x)}function A_(){ms.stop()}function D_(){ms.start()}let ms=new ww;ms.setAnimationLoop(zw),typeof self<"u"&&ms.setContext(self),this.setAnimationLoop=function(x){Lp=x,be.setAnimationLoop(x),x===null?ms.stop():ms.start()},be.addEventListener("sessionstart",A_),be.addEventListener("sessionend",D_),this.render=function(x,N){if(N!==void 0&&N.isCamera!==!0){Le("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(O===!0)return;C!==null&&C.renderStart(x,N);let j=be.enabled===!0&&be.isPresenting===!0,H=T!==null&&(X===null||j)&&T.begin(R,X);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),be.enabled===!0&&be.isPresenting===!0&&(T===null||T.isCompositing()===!1)&&(be.cameraAutoUpdate===!0&&be.updateCamera(N),N=be.getCamera()),x.isScene===!0&&x.onBeforeRender(R,x,N,X),E=ce.get(x,_.length),E.init(N),E.state.textureUnits=q.getTextureUnits(),_.push(E),et.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),Je.setFromProjectionMatrix(et,Ri,N.reversedDepth),It=this.localClippingEnabled,ut=we.init(this.clippingPlanes,It),M=de.get(x,I.length),M.init(),I.push(M),be.enabled===!0&&be.isPresenting===!0){let ye=R.xr.getDepthSensingMesh();ye!==null&&Op(ye,N,-1/0,R.sortObjects)}Op(x,N,0,R.sortObjects),M.finish(),C!==null&&C.updateLights(E.state.lightsArray),R.sortObjects===!0&&M.sort(_e,Ve),Pt=be.enabled===!1||be.isPresenting===!1||be.hasDepthSensing()===!1,Pt&&We.addToRenderList(M,x),this.info.render.frame++,this.info.autoReset===!0&&this.info.reset(),ut===!0&&we.beginShadows();let z=E.state.shadowsArray;if(Ne.render(z,x,N),ut===!0&&we.endShadows(),(H&&T.hasRenderPass())===!1){let ye=M.opaque,he=M.transmissive;if(E.setupLights(),N.isArrayCamera){let xe=N.cameras;if(he.length>0)for(let Me=0,$e=xe.length;Me<$e;Me++){let Qe=xe[Me];N_(ye,he,x,Qe)}Pt&&We.render(x);for(let Me=0,$e=xe.length;Me<$e;Me++){let Qe=xe[Me];R_(M,x,Qe,Qe.viewport)}}else he.length>0&&N_(ye,he,x,N),Pt&&We.render(x),R_(M,x,N)}X!==null&&B===0&&(q.updateMultisampleRenderTarget(X),q.updateRenderTargetMipmap(X)),H&&T.end(R),x.isScene===!0&&x.onAfterRender(R,x,N),me.resetDefaultState(),W=-1,Q=null,_.pop(),_.length>0?(E=_[_.length-1],q.setTextureUnits(E.state.textureUnits),ut===!0&&we.setGlobalState(R.clippingPlanes,E.state.camera)):E=null,I.pop(),I.length>0?M=I[I.length-1]:M=null,C!==null&&C.renderEnd()};function Op(x,N,j,H){if(x.visible===!1)return;if(x.layers.test(N.layers)){if(x.isGroup)j=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(N);else if(x.isLightProbeGrid)E.pushLightProbeGrid(x);else if(x.isLight)E.pushLight(x),x.castShadow&&E.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||x.intersectsFrustum(Je)){H&&en.setFromMatrixPosition(x.matrixWorld).applyMatrix4(et);let ye=J.update(x),he=x.material;he.visible&&M.push(x,ye,he,j,en.z,null,N)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||x.intersectsFrustum(Je))){let ye=J.update(x),he=x.material;if(H&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),en.copy(x.boundingSphere.center)):(ye.boundingSphere===null&&ye.computeBoundingSphere(),en.copy(ye.boundingSphere.center)),en.applyMatrix4(x.matrixWorld).applyMatrix4(et)),Array.isArray(he)){let xe=ye.groups;for(let Me=0,$e=xe.length;Me<$e;Me++){let Qe=xe[Me],Se=he[Qe.materialIndex];Se&&Se.visible&&M.push(x,ye,Se,j,en.z,Qe,N)}}else he.visible&&M.push(x,ye,he,j,en.z,null,N)}}let pe=x.children;for(let ye=0,he=pe.length;ye<he;ye++)Op(pe[ye],N,j,H)}function R_(x,N,j,H){let{opaque:z,transmissive:pe,transparent:ye}=x;E.setupLightsView(j),ut===!0&&we.setGlobalState(R.clippingPlanes,j),H&&v.viewport(ne.copy(H)),z.length>0&&ql(z,N,j),pe.length>0&&ql(pe,N,j),ye.length>0&&ql(ye,N,j),v.buffers.depth.setTest(!0),v.buffers.depth.setMask(!0),v.buffers.color.setMask(!0),v.setPolygonOffset(!1)}function N_(x,N,j,H){if((j.isScene===!0?j.overrideMaterial:null)!==null)return;if(E.state.transmissionRenderTarget[H.id]===void 0){let Se=xt.has("EXT_color_buffer_half_float")||xt.has("EXT_color_buffer_float");E.state.transmissionRenderTarget[H.id]=new Un(1,1,{generateMipmaps:!0,type:Se?Ui:ei,minFilter:fs,samples:Math.max(4,w.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,storeMultisampledDepthBuffer:!1,storeMultisampledStencilBuffer:!1,colorSpace:rt.workingColorSpace})}let pe=E.state.transmissionRenderTarget[H.id],ye=H.viewport||ne;pe.setSize(ye.z*R.transmissionResolutionScale,ye.w*R.transmissionResolutionScale);let he=R.getRenderTarget(),xe=R.getActiveCubeFace(),Me=R.getActiveMipmapLevel();R.setRenderTarget(pe),R.getClearColor(Ct),ot=R.getClearAlpha(),ot<1&&R.setClearColor(16777215,.5),R.clear(),Pt&&We.render(j);let $e=R.toneMapping;R.toneMapping=Li;let Qe=H.viewport;if(H.viewport!==void 0&&(H.viewport=void 0),E.setupLightsView(H),ut===!0&&we.setGlobalState(R.clippingPlanes,H),ql(x,j,H),q.updateMultisampleRenderTarget(pe),q.updateRenderTargetMipmap(pe),xt.has("WEBGL_multisampled_render_to_texture")===!1){let Se=!1;for(let gt=0,zt=N.length;gt<zt;gt++){let Rt=N[gt],{object:Tt,geometry:gn,material:ge,group:Tn}=Rt;if(ge.side===Qn&&Tt.layers.test(H.layers)){let ct=ge.side;ge.side=Pn,ge.needsUpdate=!0,P_(Tt,j,H,gn,ge,Tn),ge.side=ct,ge.needsUpdate=!0,Se=!0}}Se===!0&&(q.updateMultisampleRenderTarget(pe),q.updateRenderTargetMipmap(pe))}R.setRenderTarget(he,xe,Me),R.setClearColor(Ct,ot),Qe!==void 0&&(H.viewport=Qe),R.toneMapping=$e}function ql(x,N,j){let H=N.isScene===!0?N.overrideMaterial:null;for(let z=0,pe=x.length;z<pe;z++){let ye=x[z],{object:he,geometry:xe,group:Me}=ye,$e=ye.material;$e.allowOverride===!0&&H!==null&&($e=H),he.layers.test(j.layers)&&P_(he,N,j,xe,$e,Me)}}function P_(x,N,j,H,z,pe){C!==null&&z.isNodeMaterial&&C.setObject(x,z),x.onBeforeRender(R,N,j,H,z,pe),x.modelViewMatrix.multiplyMatrices(j.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),z.onBeforeRender(R,N,j,H,x,pe),z.transparent===!0&&z.side===Qn&&z.forceSinglePass===!1?(z.side=Pn,z.needsUpdate=!0,R.renderBufferDirect(j,N,H,z,x,pe),z.side=us,z.needsUpdate=!0,R.renderBufferDirect(j,N,H,z,x,pe),z.side=Qn):R.renderBufferDirect(j,N,H,z,x,pe),x.onAfterRender(R,N,j,H,z,pe)}function Xl(x,N,j){N.isScene!==!0&&(N=Ln);let H=G.get(x),z=E.state.lights,pe=E.state.shadowsArray,ye=z.state.version,he=ae.getParameters(x,z.state,pe,N,j,E.state.lightProbeGridArray),xe=ae.getProgramCacheKey(he),Me=H.programs;H.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?N.environment:null,H.fog=N.fog;let $e=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;H.envMap=ie.get(x.envMap||H.environment,$e),H.envMapRotation=H.environment!==null&&x.envMap===null?N.environmentRotation:x.envMapRotation,Me===void 0&&(x.addEventListener("dispose",ki),Me=new Map,H.programs=Me);let Qe=Me.get(xe);if(Qe!==void 0){if(H.currentProgram===Qe&&H.lightsStateVersion===ye)return O_(x,he),Qe}else he.uniforms=ae.getUniforms(x),C!==null&&x.isNodeMaterial&&C.build(x,j,he),x.onBeforeCompile(he,R),Qe=ae.acquireProgram(he,xe),Me.set(xe,Qe),H.uniforms=he.uniforms;let Se=H.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Se.clippingPlanes=we.uniform),O_(x,he),H.needsLights=$w(x),H.lightsStateVersion=ye,H.needsLights&&(Se.ambientLightColor.value=z.state.ambient,Se.lightProbe.value=z.state.probe,Se.sunLights.value=z.state.sun,Se.sunLightShadows.value=z.state.sunShadow,Se.directionalLights.value=z.state.directional,Se.directionalLightShadows.value=z.state.directionalShadow,Se.spotLights.value=z.state.spot,Se.spotLightShadows.value=z.state.spotShadow,Se.rectAreaLights.value=z.state.rectArea,Se.ltc_1.value=z.state.rectAreaLTC1,Se.ltc_2.value=z.state.rectAreaLTC2,Se.pointLights.value=z.state.point,Se.pointLightShadows.value=z.state.pointShadow,Se.hemisphereLights.value=z.state.hemi,Se.sunShadowMatrix.value=z.state.sunShadowMatrix,Se.sunShadowCascade.value=z.state.sunShadowCascade,Se.directionalShadowMatrix.value=z.state.directionalShadowMatrix,Se.spotLightMatrix.value=z.state.spotLightMatrix,Se.spotLightMap.value=z.state.spotLightMap,Se.pointShadowMatrix.value=z.state.pointShadowMatrix),H.lightProbeGrid=E.state.lightProbeGridArray.length>0,H.currentProgram=Qe,H.uniformsList=null,Qe}function L_(x){if(x.uniformsList===null){let N=x.currentProgram.getUniforms();x.uniformsList=Oa.seqWithValue(N.seq,x.uniforms)}return x.uniformsList}function O_(x,N){let j=G.get(x);j.outputColorSpace=N.outputColorSpace,j.batching=N.batching,j.batchingColor=N.batchingColor,j.instancing=N.instancing,j.instancingColor=N.instancingColor,j.instancingMorph=N.instancingMorph,j.skinning=N.skinning,j.morphTargets=N.morphTargets,j.morphNormals=N.morphNormals,j.morphColors=N.morphColors,j.morphTargetsCount=N.morphTargetsCount,j.numClippingPlanes=N.numClippingPlanes,j.numIntersection=N.numClipIntersection,j.vertexAlphas=N.vertexAlphas,j.vertexTangents=N.vertexTangents,j.toneMapping=N.toneMapping}function Gw(x,N){if(x.length===0)return null;if(x.length===1)return x[0].texture!==null?x[0]:null;S.setFromMatrixPosition(N.matrixWorld);for(let j=0,H=x.length;j<H;j++){let z=x[j];if(z.texture!==null&&z.boundingBox.containsPoint(S))return z}return null}function Ww(x,N,j,H,z){N.isScene!==!0&&(N=Ln),q.resetTextureUnits();let pe=N.fog,ye=H.isMeshStandardMaterial||H.isMeshLambertMaterial||H.isMeshPhongMaterial?N.environment:null,he=X===null?R.outputColorSpace:X.isXRRenderTarget===!0?X.texture.colorSpace:rt.workingColorSpace,xe=H.isMeshStandardMaterial||H.isMeshLambertMaterial&&!H.envMap||H.isMeshPhongMaterial&&!H.envMap,Me=ie.get(H.envMap||ye,xe),$e=H.vertexColors===!0&&!!j.attributes.color&&j.attributes.color.itemSize===4,Qe=!!j.attributes.tangent&&(!!H.normalMap||H.anisotropy>0),Se=!!j.morphAttributes.position,gt=!!j.morphAttributes.normal,zt=!!j.morphAttributes.color,Rt=Li;H.toneMapped&&(X===null||X.isXRRenderTarget===!0)&&(Rt=R.toneMapping);let Tt=j.morphAttributes.position||j.morphAttributes.normal||j.morphAttributes.color,gn=Tt!==void 0?Tt.length:0,ge=G.get(H),Tn=E.state.lights;if(ut===!0&&(It===!0||x!==Q)){let Dt=x===Q&&H.id===W;we.setState(H,x,Dt)}let ct=!1;H.version===ge.__version?(ge.needsLights&&ge.lightsStateVersion!==Tn.state.version||ge.outputColorSpace!==he||z.isBatchedMesh&&ge.batching===!1||!z.isBatchedMesh&&ge.batching===!0||z.isBatchedMesh&&ge.batchingColor===!0&&z._colorsTexture===null||z.isBatchedMesh&&ge.batchingColor===!1&&z._colorsTexture!==null||z.isInstancedMesh&&ge.instancing===!1||!z.isInstancedMesh&&ge.instancing===!0||z.isSkinnedMesh&&ge.skinning===!1||!z.isSkinnedMesh&&ge.skinning===!0||z.isInstancedMesh&&ge.instancingColor===!0&&z.instanceColor===null||z.isInstancedMesh&&ge.instancingColor===!1&&z.instanceColor!==null||z.isInstancedMesh&&ge.instancingMorph===!0&&z.morphTexture===null||z.isInstancedMesh&&ge.instancingMorph===!1&&z.morphTexture!==null||ge.envMap!==Me||H.fog===!0&&ge.fog!==pe||ge.numClippingPlanes!==void 0&&(ge.numClippingPlanes!==we.numPlanes||ge.numIntersection!==we.numIntersection)||ge.vertexAlphas!==$e||ge.vertexTangents!==Qe||ge.morphTargets!==Se||ge.morphNormals!==gt||ge.morphColors!==zt||ge.toneMapping!==Rt||ge.morphTargetsCount!==gn||!!ge.lightProbeGrid!=E.state.lightProbeGridArray.length>0)&&(ct=!0):(ct=!0,ge.__version=H.version);let ti=ge.currentProgram;ct===!0&&(ti=Xl(H,N,z),C&&H.isNodeMaterial&&C.onUpdateProgram(H,ti,ge));let Bi=!1,Dr=!1,ao=!1,Mt=ti.getUniforms(),Bt=ge.uniforms;if(v.useProgram(ti.program)&&(Bi=!0,Dr=!0,ao=!0),H.id!==W&&(W=H.id,Dr=!0),ge.needsLights){let Dt=Gw(E.state.lightProbeGridArray,z);ge.lightProbeGrid!==Dt&&(ge.lightProbeGrid=Dt,Dr=!0)}if(Bi||Q!==x){v.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),Mt.setValue(L,"projectionMatrix",x.projectionMatrix),Mt.setValue(L,"viewMatrix",x.matrixWorldInverse);let Nr=Mt.map.cameraPosition;Nr!==void 0&&Nr.setValue(L,Nt.setFromMatrixPosition(x.matrixWorld)),w.logarithmicDepthBuffer&&Mt.setValue(L,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(H.isMeshPhongMaterial||H.isMeshToonMaterial||H.isMeshLambertMaterial||H.isMeshBasicMaterial||H.isMeshStandardMaterial||H.isShaderMaterial)&&Mt.setValue(L,"isOrthographic",x.isOrthographicCamera===!0),Q!==x&&(Q=x,Dr=!0,ao=!0)}if(ge.needsLights&&(Tn.state.sunShadowMap.length>0&&Mt.setValue(L,"sunShadowMap",Tn.state.sunShadowMap,q),Tn.state.directionalShadowMap.length>0&&Mt.setValue(L,"directionalShadowMap",Tn.state.directionalShadowMap,q),Tn.state.spotShadowMap.length>0&&Mt.setValue(L,"spotShadowMap",Tn.state.spotShadowMap,q),Tn.state.pointShadowMap.length>0&&Mt.setValue(L,"pointShadowMap",Tn.state.pointShadowMap,q)),z.isSkinnedMesh){Mt.setOptional(L,z,"bindMatrix"),Mt.setOptional(L,z,"bindMatrixInverse");let Dt=z.skeleton;Dt&&(Dt.boneTexture===null&&Dt.computeBoneTexture(),Mt.setValue(L,"boneTexture",Dt.boneTexture,q))}z.isBatchedMesh&&(Mt.setOptional(L,z,"batchingTexture"),Mt.setValue(L,"batchingTexture",z._matricesTexture,q),Mt.setOptional(L,z,"batchingIdTexture"),Mt.setValue(L,"batchingIdTexture",z._indirectTexture,q),Mt.setOptional(L,z,"batchingColorTexture"),z._colorsTexture!==null&&Mt.setValue(L,"batchingColorTexture",z._colorsTexture,q));let Rr=j.morphAttributes;if((Rr.position!==void 0||Rr.normal!==void 0||Rr.color!==void 0)&&P.update(z,j,ti),(Dr||ge.receiveShadow!==z.receiveShadow)&&(ge.receiveShadow=z.receiveShadow,Mt.setValue(L,"receiveShadow",z.receiveShadow)),(H.isMeshStandardMaterial||H.isMeshLambertMaterial||H.isMeshPhongMaterial)&&H.envMap===null&&N.environment!==null&&(Bt.envMapIntensity.value=N.environmentIntensity),Bt.dfgLUT!==void 0&&(Bt.dfgLUT.value=ok()),Dr){if(Mt.setValue(L,"toneMappingExposure",R.toneMappingExposure),ge.needsLights&&jw(Bt,ao),pe&&H.fog===!0&&Ee.refreshFogUniforms(Bt,pe),Ee.refreshMaterialUniforms(Bt,H,ee,Y,E.state.transmissionRenderTarget[x.id]),ge.needsLights&&ge.lightProbeGrid){let Dt=ge.lightProbeGrid;Bt.probesSH.value=Dt.texture,Bt.probesMin.value.copy(Dt.boundingBox.min),Bt.probesMax.value.copy(Dt.boundingBox.max),Bt.probesResolution.value.copy(Dt.resolution)}Oa.upload(L,L_(ge),Bt,q)}if(H.isShaderMaterial&&H.uniformsNeedUpdate===!0&&(Oa.upload(L,L_(ge),Bt,q),H.uniformsNeedUpdate=!1),H.isSpriteMaterial&&Mt.setValue(L,"center",z.center),Mt.setValue(L,"modelViewMatrix",z.modelViewMatrix),Mt.setValue(L,"normalMatrix",z.normalMatrix),Mt.setValue(L,"modelMatrix",z.matrixWorld),H.uniformsGroups!==void 0){let Dt=H.uniformsGroups;for(let Nr=0,co=Dt.length;Nr<co;Nr++){let U_=Dt[Nr];te.update(U_,ti),te.bind(U_,ti)}}return ti}function jw(x,N){x.ambientLightColor.needsUpdate=N,x.lightProbe.needsUpdate=N,x.sunLights.needsUpdate=N,x.sunLightShadows.needsUpdate=N,x.directionalLights.needsUpdate=N,x.directionalLightShadows.needsUpdate=N,x.pointLights.needsUpdate=N,x.pointLightShadows.needsUpdate=N,x.spotLights.needsUpdate=N,x.spotLightShadows.needsUpdate=N,x.rectAreaLights.needsUpdate=N,x.hemisphereLights.needsUpdate=N}function $w(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return $},this.getActiveMipmapLevel=function(){return B},this.getRenderTarget=function(){return X},this.setRenderTargetTextures=function(x,N,j){let H=G.get(x);H.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,H.__autoAllocateDepthBuffer===!1&&(H.__useRenderToTexture=!1),G.get(x.texture).__webglTexture=N,G.get(x.depthTexture).__webglTexture=H.__autoAllocateDepthBuffer?void 0:j,H.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,N){let j=G.get(x);j.__webglFramebuffer=N,j.__useDefaultFramebuffer=N===void 0},this.setRenderTarget=function(x,N=0,j=0){X=x,$=N,B=j;let H=null,z=!1,pe=!1;if(x){let he=G.get(x);if(he.__useDefaultFramebuffer!==void 0){v.bindFramebuffer(L.FRAMEBUFFER,he.__webglFramebuffer),ne.copy(x.viewport),Ie.copy(x.scissor),Te=x.scissorTest,v.viewport(ne),v.scissor(Ie),v.setScissorTest(Te),W=-1;return}else if(he.__webglFramebuffer===void 0)q.setupRenderTarget(x);else if(he.__hasExternalTextures)q.rebindTextures(x,G.get(x.texture).__webglTexture,G.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let $e=x.depthTexture;if(he.__boundDepthTexture!==$e){if($e!==null&&G.has($e)&&(x.width!==$e.image.width||x.height!==$e.image.height))throw new Error("THREE.WebGLRenderer: Attached DepthTexture is initialized to the incorrect size.");q.setupDepthRenderbuffer(x)}}let xe=x.texture;(xe.isData3DTexture||xe.isDataArrayTexture||xe.isCompressedArrayTexture)&&(pe=!0);let Me=G.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(Me[N])?H=Me[N][j]:H=Me[N],z=!0):x.samples>0&&q.useMultisampledRTT(x)===!1?H=G.get(x).__webglMultisampledFramebuffer:Array.isArray(Me)?H=Me[j]:H=Me,ne.copy(x.viewport),Ie.copy(x.scissor),Te=x.scissorTest}else ne.copy(ve).multiplyScalar(ee).floor(),Ie.copy(Ye).multiplyScalar(ee).floor(),Te=$t;if(j!==0&&(H=V),v.bindFramebuffer(L.FRAMEBUFFER,H)&&v.drawBuffers(x,H),v.viewport(ne),v.scissor(Ie),v.setScissorTest(Te),z){let he=G.get(x.texture);L.framebufferTexture2D(L.FRAMEBUFFER,L.COLOR_ATTACHMENT0,L.TEXTURE_CUBE_MAP_POSITIVE_X+N,he.__webglTexture,j)}else if(pe){let he=N;for(let xe=0;xe<x.textures.length;xe++){let Me=G.get(x.textures[xe]);L.framebufferTextureLayer(L.FRAMEBUFFER,L.COLOR_ATTACHMENT0+xe,Me.__webglTexture,j,he)}}else if(x!==null&&j!==0){let he=G.get(x.texture);L.framebufferTexture2D(L.FRAMEBUFFER,L.COLOR_ATTACHMENT0,L.TEXTURE_2D,he.__webglTexture,j)}W=-1};function F_(x){let N=G.get(x);return(N.__readFormat!==x.format||N.__readType!==x.type)&&(N.__readFormat=x.format,N.__readType=x.type,N.__formatReadable=w.textureFormatReadable(x.format),N.__typeReadable=w.textureTypeReadable(x.type)),N}this.readRenderTargetPixels=function(x,N,j,H,z,pe,ye,he=0){if(!(x&&x.isWebGLRenderTarget)){Le("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let xe=G.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&ye!==void 0&&(xe=xe[ye]),xe){v.bindFramebuffer(L.FRAMEBUFFER,xe);try{let Me=x.textures[he],$e=Me.format,Qe=Me.type;x.textures.length>1&&L.readBuffer(L.COLOR_ATTACHMENT0+he);let Se=F_(Me);if(Se.__formatReadable===!1){Le("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(Se.__typeReadable===!1){Le("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=x.width-H&&j>=0&&j<=x.height-z&&L.readPixels(N,j,H,z,ue.convert($e),ue.convert(Qe),pe)}finally{let Me=X!==null?G.get(X).__webglFramebuffer:null;v.bindFramebuffer(L.FRAMEBUFFER,Me)}}},this.readRenderTargetPixelsAsync=async function(x,N,j,H,z,pe,ye,he=0){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let xe=G.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&ye!==void 0&&(xe=xe[ye]),xe)if(N>=0&&N<=x.width-H&&j>=0&&j<=x.height-z){v.bindFramebuffer(L.FRAMEBUFFER,xe);let Me=x.textures[he],$e=Me.format,Qe=Me.type;x.textures.length>1&&L.readBuffer(L.COLOR_ATTACHMENT0+he);let Se=F_(Me);if(Se.__formatReadable===!1)throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(Se.__typeReadable===!1)throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let gt=L.createBuffer();L.bindBuffer(L.PIXEL_PACK_BUFFER,gt),L.bufferData(L.PIXEL_PACK_BUFFER,pe.byteLength,L.STREAM_READ),L.readPixels(N,j,H,z,ue.convert($e),ue.convert(Qe),0),L.bindBuffer(L.PIXEL_PACK_BUFFER,null);let zt=X!==null?G.get(X).__webglFramebuffer:null;v.bindFramebuffer(L.FRAMEBUFFER,zt);let Rt=L.fenceSync(L.SYNC_GPU_COMMANDS_COMPLETE,0);return L.flush(),await JE(L,Rt,4),L.bindBuffer(L.PIXEL_PACK_BUFFER,gt),L.getBufferSubData(L.PIXEL_PACK_BUFFER,0,pe),L.bindBuffer(L.PIXEL_PACK_BUFFER,null),L.deleteBuffer(gt),L.deleteSync(Rt),pe}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(x,N=null,j=0){let H=Math.pow(2,-j),z=Math.floor(x.image.width*H),pe=Math.floor(x.image.height*H),ye=N!==null?N.x:0,he=N!==null?N.y:0;q.setTexture2D(x,0),L.copyTexSubImage2D(L.TEXTURE_2D,j,0,0,ye,he,z,pe),v.unbindTexture()},this.copyTextureToTexture=function(x,N,j=null,H=null,z=0,pe=0){let ye,he,xe,Me,$e,Qe,Se,gt,zt,Rt=x.isCompressedTexture?x.mipmaps[pe]:x.image;if(j!==null)ye=j.max.x-j.min.x,he=j.max.y-j.min.y,xe=j.isBox3?j.max.z-j.min.z:1,Me=j.min.x,$e=j.min.y,Qe=j.isBox3?j.min.z:0;else{let Bt=Math.pow(2,-z);ye=Math.floor(Rt.width*Bt),he=Math.floor(Rt.height*Bt),x.isDataArrayTexture?xe=Rt.depth:x.isData3DTexture?xe=Math.floor(Rt.depth*Bt):xe=1,Me=0,$e=0,Qe=0}H!==null?(Se=H.x,gt=H.y,zt=H.z):(Se=0,gt=0,zt=0);let Tt=ue.convert(N.format),gn=ue.convert(N.type),ge;N.isData3DTexture?(q.setTexture3D(N,0),ge=L.TEXTURE_3D):N.isDataArrayTexture||N.isCompressedArrayTexture?(q.setTexture2DArray(N,0),ge=L.TEXTURE_2D_ARRAY):(q.setTexture2D(N,0),ge=L.TEXTURE_2D),v.activeTexture(L.TEXTURE0),v.pixelStorei(L.UNPACK_FLIP_Y_WEBGL,N.flipY),v.pixelStorei(L.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),v.pixelStorei(L.UNPACK_ALIGNMENT,N.unpackAlignment);let Tn=v.getParameter(L.UNPACK_ROW_LENGTH),ct=v.getParameter(L.UNPACK_IMAGE_HEIGHT),ti=v.getParameter(L.UNPACK_SKIP_PIXELS),Bi=v.getParameter(L.UNPACK_SKIP_ROWS),Dr=v.getParameter(L.UNPACK_SKIP_IMAGES);v.pixelStorei(L.UNPACK_ROW_LENGTH,Rt.width),v.pixelStorei(L.UNPACK_IMAGE_HEIGHT,Rt.height),v.pixelStorei(L.UNPACK_SKIP_PIXELS,Me),v.pixelStorei(L.UNPACK_SKIP_ROWS,$e),v.pixelStorei(L.UNPACK_SKIP_IMAGES,Qe);let ao=x.isDataArrayTexture||x.isData3DTexture,Mt=N.isDataArrayTexture||N.isData3DTexture;if(x.isDepthTexture){let Bt=G.get(x),Rr=G.get(N),Dt=G.get(Bt.__renderTarget),Nr=G.get(Rr.__renderTarget);v.bindFramebuffer(L.READ_FRAMEBUFFER,Dt.__webglFramebuffer),v.bindFramebuffer(L.DRAW_FRAMEBUFFER,Nr.__webglFramebuffer);for(let co=0;co<xe;co++)ao&&(L.framebufferTextureLayer(L.READ_FRAMEBUFFER,L.COLOR_ATTACHMENT0,G.get(x).__webglTexture,z,Qe+co),L.framebufferTextureLayer(L.DRAW_FRAMEBUFFER,L.COLOR_ATTACHMENT0,G.get(N).__webglTexture,pe,zt+co)),L.blitFramebuffer(Me,$e,ye,he,Se,gt,ye,he,L.DEPTH_BUFFER_BIT,L.NEAREST);v.bindFramebuffer(L.READ_FRAMEBUFFER,null),v.bindFramebuffer(L.DRAW_FRAMEBUFFER,null)}else if(z!==0||x.isRenderTargetTexture||G.has(x)){let Bt=G.get(x),Rr=G.get(N);v.bindFramebuffer(L.READ_FRAMEBUFFER,D),v.bindFramebuffer(L.DRAW_FRAMEBUFFER,U);for(let Dt=0;Dt<xe;Dt++)ao?L.framebufferTextureLayer(L.READ_FRAMEBUFFER,L.COLOR_ATTACHMENT0,Bt.__webglTexture,z,Qe+Dt):L.framebufferTexture2D(L.READ_FRAMEBUFFER,L.COLOR_ATTACHMENT0,L.TEXTURE_2D,Bt.__webglTexture,z),Mt?L.framebufferTextureLayer(L.DRAW_FRAMEBUFFER,L.COLOR_ATTACHMENT0,Rr.__webglTexture,pe,zt+Dt):L.framebufferTexture2D(L.DRAW_FRAMEBUFFER,L.COLOR_ATTACHMENT0,L.TEXTURE_2D,Rr.__webglTexture,pe),z!==0?L.blitFramebuffer(Me,$e,ye,he,Se,gt,ye,he,L.COLOR_BUFFER_BIT,L.NEAREST):Mt?L.copyTexSubImage3D(ge,pe,Se,gt,zt+Dt,Me,$e,ye,he):L.copyTexSubImage2D(ge,pe,Se,gt,Me,$e,ye,he);v.bindFramebuffer(L.READ_FRAMEBUFFER,null),v.bindFramebuffer(L.DRAW_FRAMEBUFFER,null)}else Mt?x.isDataTexture||x.isData3DTexture?L.texSubImage3D(ge,pe,Se,gt,zt,ye,he,xe,Tt,gn,Rt.data):N.isCompressedArrayTexture?L.compressedTexSubImage3D(ge,pe,Se,gt,zt,ye,he,xe,Tt,Rt.data):L.texSubImage3D(ge,pe,Se,gt,zt,ye,he,xe,Tt,gn,Rt):x.isDataTexture?L.texSubImage2D(L.TEXTURE_2D,pe,Se,gt,ye,he,Tt,gn,Rt.data):x.isCompressedTexture?L.compressedTexSubImage2D(L.TEXTURE_2D,pe,Se,gt,Rt.width,Rt.height,Tt,Rt.data):L.texSubImage2D(L.TEXTURE_2D,pe,Se,gt,ye,he,Tt,gn,Rt);v.pixelStorei(L.UNPACK_ROW_LENGTH,Tn),v.pixelStorei(L.UNPACK_IMAGE_HEIGHT,ct),v.pixelStorei(L.UNPACK_SKIP_PIXELS,ti),v.pixelStorei(L.UNPACK_SKIP_ROWS,Bi),v.pixelStorei(L.UNPACK_SKIP_IMAGES,Dr),pe===0&&N.generateMipmaps&&L.generateMipmap(ge),v.unbindTexture()},this.initRenderTarget=function(x){G.get(x).__webglFramebuffer===void 0&&q.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?q.setTextureCube(x,0):x.isData3DTexture?q.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?q.setTexture2DArray(x,0):q.setTexture2D(x,0),v.unbindTexture()},this.resetState=function(){$=0,B=0,X=null,v.reset(),me.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Ri}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=rt._getDrawingBufferColorSpace(e),t.unpackColorSpace=rt._getUnpackColorSpace()}};var Nw={type:"change"},E_={type:"start"},Lw={type:"end"},Rp=new eo,Pw=new Yn,ck=Math.cos(70*i_.DEG2RAD),Qt=new F,kn=2*Math.PI,bt={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},M_=1e-6,Np=class extends Pl{constructor(e,t=null){super(e,t),this.state=bt.NONE,this.target=new F,this.cursor=new F,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:cs.ROTATE,MIDDLE:cs.DOLLY,RIGHT:cs.PAN},this.touches={ONE:ls.ROTATE,TWO:ls.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new F,this._lastQuaternion=new Zn,this._lastTargetPosition=new F,this._quat=new Zn().setFromUnitVectors(e.up,new F(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new Ia,this._sphericalDelta=new Ia,this._scale=1,this._panOffset=new F,this._rotateStart=new Pe,this._rotateEnd=new Pe,this._rotateDelta=new Pe,this._panStart=new Pe,this._panEnd=new Pe,this._panDelta=new Pe,this._dollyStart=new Pe,this._dollyEnd=new Pe,this._dollyDelta=new Pe,this._dollyDirection=new F,this._mouse=new Pe,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=uk.bind(this),this._onPointerDown=lk.bind(this),this._onPointerUp=dk.bind(this),this._onContextMenu=yk.bind(this),this._onMouseWheel=pk.bind(this),this._onKeyDown=mk.bind(this),this._onTouchStart=gk.bind(this),this._onTouchMove=vk.bind(this),this._onMouseDown=fk.bind(this),this._onMouseMove=hk.bind(this),this._interceptControlDown=_k.bind(this),this._interceptControlUp=xk.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.state=bt.NONE,this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents();let e=this.domElement.getRootNode();e.removeEventListener("keydown",this._interceptControlDown,{capture:!0}),e.removeEventListener("keyup",this._interceptControlUp,{capture:!0}),this._controlActive=!1,this._pointers.length=0,this._pointerPositions={},this.domElement.style.touchAction="",this.domElement.style.cursor="auto"}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(Nw),this.update(),this.state=bt.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;Qt.copy(t).sub(this.target),Qt.applyQuaternion(this._quat),this._spherical.setFromVector3(Qt),this.autoRotate&&this.state===bt.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=kn:i>Math.PI&&(i-=kn),r<-Math.PI?r+=kn:r>Math.PI&&(r-=kn),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(Qt.setFromSpherical(this._spherical),Qt.applyQuaternion(this._quatInverse),t.copy(this.target).add(Qt),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=Qt.length();o=this._clampDistance(a*this._scale);let c=a-o;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),s=!!c}else if(this.object.isOrthographicCamera){let a=new F(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=c!==this.object.zoom;let l=new F(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),o=Qt.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(Rp.origin.copy(this.object.position),Rp.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(Rp.direction))<ck?this.object.lookAt(this.target):(Pw.setFromNormalAndCoplanarPoint(this.object.up,this.target),Rp.intersectPlane(Pw,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>M_||8*(1-this._lastQuaternion.dot(this.object.quaternion))>M_||this._lastTargetPosition.distanceToSquared(this.target)>M_?(this.dispatchEvent(Nw),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?kn/60*this.autoRotateSpeed*e:kn/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){Qt.setFromMatrixColumn(t,0),Qt.multiplyScalar(-e),this._panOffset.add(Qt)}_panUp(e,t){this.screenSpacePanning===!0?Qt.setFromMatrixColumn(t,1):(Qt.setFromMatrixColumn(t,0),Qt.crossVectors(this.object.up,Qt)),Qt.multiplyScalar(e),this._panOffset.add(Qt)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;Qt.copy(r).sub(this.target);let s=Qt.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(kn*this._rotateDelta.x/t.clientHeight),this._rotateUp(kn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(kn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-kn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(kn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-kn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(kn*this._rotateDelta.x/t.clientHeight),this._rotateUp(kn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new Pe,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function lk(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function uk(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function dk(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(Lw),this.state=bt.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function fk(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case cs.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=bt.DOLLY;break;case cs.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=bt.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=bt.ROTATE}break;case cs.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=bt.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=bt.PAN}break;default:this.state=bt.NONE}this.state!==bt.NONE&&this.dispatchEvent(E_)}function hk(n){switch(this.state){case bt.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case bt.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case bt.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function pk(n){this.enabled===!1||this.enableZoom===!1||this.state!==bt.NONE||(n.preventDefault(),this.dispatchEvent(E_),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(Lw))}function mk(n){this.enabled!==!1&&this._handleKeyDown(n)}function gk(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case ls.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=bt.TOUCH_ROTATE;break;case ls.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=bt.TOUCH_PAN;break;default:this.state=bt.NONE}break;case 2:switch(this.touches.TWO){case ls.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=bt.TOUCH_DOLLY_PAN;break;case ls.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=bt.TOUCH_DOLLY_ROTATE;break;default:this.state=bt.NONE}break;default:this.state=bt.NONE}this.state!==bt.NONE&&this.dispatchEvent(E_)}function vk(n){switch(this._trackPointer(n),this.state){case bt.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case bt.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case bt.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case bt.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=bt.NONE}}function yk(n){this.enabled!==!1&&n.preventDefault()}function _k(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function xk(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var Ow={width:130,arc:180,hole:30,baseH:3,border:2,minT:.8,maxT:3,pixel:.2,gamma:1,invert:!1,mode:"single",stripe:2,layerH:.2,baseShape:"semi"},Sk=["Red","Green","Blue"],bk=[[1,.12,.12],[.12,1,.12],[.15,.25,1]],Mk=5,Ek=6;function wk(n,e,t,i){let r=[{c:0,z:0,fl:1}],s=-1,o=1;for(;;){let a=Math.max(Math.round((o*e-i)/t),s+1),c=i+a*t;if(c>n-t*.5)break;r.push({c:o%3,z:c,fl:a+2}),s=a,o++}return r}var Ua=null;function w_(n,e){if(e.mode==="rgb"&&e.stripe<e.layerH)throw new Error("Stripe height cannot be below the layer height.");let t=e.border>0?Math.max(1,Math.round(e.border/e.pixel)):0,i=Math.max(2,Math.round(e.width/e.pixel)+1),r=Math.max(2,Math.round((i-1)*n.naturalHeight/n.naturalWidth)+1),s;if(Ua&&Ua.img===n&&Ua.c===i&&Ua.r===r)s=Ua.data;else{let m=document.createElement("canvas");m.width=i,m.height=r;let y=m.getContext("2d",{willReadFrequently:!0});y.fillStyle="#fff",y.fillRect(0,0,i,r),y.drawImage(n,0,0,i,r),s=y.getImageData(0,0,i,r).data,Ua={img:n,c:i,r,data:s}}let o=r+2*t,a=i+2*t,c=e.baseH-Math.min(1,e.baseH/2),l=c+(o-1)*e.pixel,u=[],d=new Uint8Array(o);if(e.mode==="rgb"){u=wk(l,e.stripe,e.layerH,e.layerH);let m=u.slice(1).map(y=>y.z);for(let y=0;y<o;y++){let g=c+(o-1-y)*e.pixel+1e-6,p=0;for(;p<m.length&&m[p]<=g;)p++;d[y]=p%3}}let f=new Float32Array(o*a).fill(e.maxT),h=new Float32Array(o*a*3).fill(.03);for(let m=0;m<r;m++)for(let y=0;y<i;y++){let g=(m+t)*a+y+t,p=(m*i+y)*4,b=[0,1,2].map(E=>{let I=s[p+E]/255;return e.invert&&(I=1-I),e.gamma!==1?Math.pow(I,e.gamma):I}),A=d[m+t],S=e.mode==="rgb"?b[A]:.299*b[0]+.587*b[1]+.114*b[2];f[g]=e.minT+(e.maxT-e.minT)*(1-S);let M=e.mode==="rgb"?bk[A]:[1,.93,.78];h[g*3]=S*M[0],h[g*3+1]=S*M[1],h[g*3+2]=S*M[2]}return{rows:o,cols:a,th:f,col:h,stripes:u,height:l}}function T_(n,e){let{rows:t,cols:i,th:r,col:s}=e,o=t*i,a=n.pixel,c=(i-1)*a,l=n.arc*Math.PI/180,u=c/l,d=n.hole/2;if(d>u-2)throw new Error(`Hole too big: the inside diameter is only ${(2*u).toFixed(0)} mm. Widen the picture, lower the arc or shrink the hole.`);let f=n.baseH-Math.min(1,n.baseH/2),h=0;for(let C=0;C<o;C++)r[C]>h&&(h=r[C]);let m=u+h+Mk,y=(C,V,D,U,$)=>{let B=Math.max(24,Math.round(U*V/.4));return{rIn:C,rOut:V,a0:D,span:U,full:$,m:B,pts:$?B:B+1}},g=Math.max(Math.PI,l),p=n.baseShape==="semi"?[y(d,m,-Math.PI/2-g/2,g,!1),y(d,Math.min(m,d+Ek),0,2*Math.PI,!0)]:[y(d,m,0,2*Math.PI,!0)],b=new Float32Array((2*o+p.reduce((C,V)=>C+4*V.pts,0))*3),A=new Float32Array(b.length);for(let C=0;C<t;C++)for(let V=0;V<i;V++){let D=C*i+V,U=(V*a/c-.5)*l,$=Math.sin(U),B=Math.cos(U),X=(t-1-C)*a+f,W=u+r[D];b.set([W*$,-W*B,X],D*3),b.set([u*$,-u*B,X],(o+D)*3),A.set([s[D*3],s[D*3+1],s[D*3+2]],D*3),A.fill(.12,(o+D)*3,(o+D)*3+3)}let S=2*o,M=p.map(C=>{let V=S,D=V+C.pts,U=D+C.pts,$=U+C.pts;S=$+C.pts;for(let B=0;B<C.pts;B++){let X=C.a0+C.span*B/C.m,W=Math.cos(X),Q=Math.sin(X);b.set([C.rOut*W,C.rOut*Q,n.baseH],(V+B)*3),b.set([C.rOut*W,C.rOut*Q,0],(D+B)*3),b.set([C.rIn*W,C.rIn*Q,n.baseH],(U+B)*3),b.set([C.rIn*W,C.rIn*Q,0],($+B)*3)}return{OT:V,OB:D,IT:U,IB:$}});A.fill(.25,2*o*3);let E=[];for(let C=0;C<i;C++)E.push((t-1)*i+C);for(let C=t-2;C>=0;C--)E.push(C*i+i-1);for(let C=i-2;C>=0;C--)E.push(C);for(let C=1;C<t-1;C++)E.push(C*i);let I=p.reduce((C,V)=>C+8*V.m+(V.full?0:4),0),_=new Uint32Array((4*(t-1)*(i-1)+2*E.length+I)*3),T=0,R=(C,V,D)=>{_[T++]=C,_[T++]=V,_[T++]=D};for(let C=0;C<t-1;C++)for(let V=0;V<i-1;V++){let D=C*i+V,U=D+1,$=D+i,B=$+1;R(D,$,B),R(D,B,U),R(o+D,o+B,o+$),R(o+D,o+U,o+B)}for(let C=0;C<E.length;C++){let V=E[C],D=E[(C+1)%E.length];R(V+o,D+o,D),R(V+o,D,V)}p.forEach((C,V)=>{let{OT:D,OB:U,IT:$,IB:B}=M[V];for(let X=0;X<C.m;X++){let W=C.full?(X+1)%C.m:X+1;R(D+X,D+W,$+W),R(D+X,$+W,$+X),R(U+X,B+W,U+W),R(U+X,B+X,B+W),R(U+X,U+W,D+W),R(U+X,D+W,D+X),R(B+X,$+W,B+W),R(B+X,$+X,$+W)}if(!C.full){let X=C.m;R(B,U,D),R(B,D,$),R(B+X,D+X,U+X),R(B+X,$+X,D+X)}});let O={x0:1/0,x1:-1/0,y0:1/0,y1:-1/0,z1:0};for(let C=0;C<b.length;C+=3)b[C]<O.x0&&(O.x0=b[C]),b[C]>O.x1&&(O.x1=b[C]),b[C+1]<O.y0&&(O.y0=b[C+1]),b[C+1]>O.y1&&(O.y1=b[C+1]),b[C+2]>O.z1&&(O.z1=b[C+2]);return{V:b,F:_,C:A,outerR:m,radius:u,height:e.height,box:O}}function Fw(n){let e=n.F.length/3,t=new ArrayBuffer(84+50*e),i=new DataView(t);i.setUint32(80,e,!0);let r=s=>[n.V[s*3]+n.outerR,n.V[s*3+1]+n.outerR,n.V[s*3+2]];for(let s=0;s<e;s++){let o=r(n.F[s*3]),a=r(n.F[s*3+1]),c=r(n.F[s*3+2]),l=[a[0]-o[0],a[1]-o[1],a[2]-o[2]],u=[c[0]-o[0],c[1]-o[1],c[2]-o[2]],d=l[1]*u[2]-l[2]*u[1],f=l[2]*u[0]-l[0]*u[2],h=l[0]*u[1]-l[1]*u[0],m=Math.hypot(d,f,h)||1,y=84+s*50;[d/m,f/m,h/m,...o,...a,...c].forEach((g,p)=>i.setFloat32(y+p*4,g,!0))}return new Blob([t],{type:"model/stl"})}function Uw(n,e){let t=e.stripes,i=["LITHOPHANE COLOR SWAP SHEET","=".repeat(50),`Layer height ${n.layerH} mm, first layer ${n.layerH} mm, 100% infill`,"Print standing exactly as exported (base on the bed).","Start with RED. Pause BEFORE the layer listed, swap, purge, resume.","","Swap  Pause before layer  Layer Z   Load"];return t.slice(1).forEach((r,s)=>i.push(`${String(s+1).padStart(4)}  ${String(r.fl).padStart(18)}  ${(n.layerH+(r.fl-1)*n.layerH).toFixed(2).padStart(7)}   ${Sk[r.c].toUpperCase()}`)),i.join(`
`)+`
`}var Tk=["cv"],Ck=["host"],Ik=(n,e)=>e.id,Ak=(n,e)=>e.k;function Dk(n,e){n&1&&(je(0,"p",6),it(1,"Half-round base under the picture, with a ring kept around the hole. It widens if the arc is over 180\xB0."),ke())}function Rk(n,e){if(n&1){let t=Tc();je(0,"button",13),Wn("click",function(){let r=Ao(t).$implicit,s=dn();return Do(s.choose(r.id))}),it(1),ke()}if(n&2){let t=e.$implicit,i=dn();mr("rounded-lg px-2 py-2 text-xs font-medium transition "+(i.printer()===t.id?"bg-amber-400 text-zinc-900":"bg-zinc-800 text-zinc-300 hover:bg-zinc-700")),at(),wi(t.name)}}function Nk(n,e){if(n&1&&(je(0,"div",25),it(1),ke()),n&2){let t=dn();at(),zs("Too large in: ",t.over.join(", "))}}function Pk(n,e){if(n&1&&(je(0,"div")(1,"div",24),it(2),ke(),je(3,"div",17),it(4),ke(),Mi(5,Nk,2,1,"div",25),ke()),n&2){let t=e,i=dn();mr("rounded-xl border p-3 text-xs "+(t.ok?"border-emerald-400/40 bg-emerald-400/10 text-emerald-200":"border-red-400/40 bg-red-400/10 text-red-200")),at(2),wi(t.ok?"Fits on the "+t.pr.name:"Too big for the "+t.pr.name),at(2),jd("",i.dims().x.toFixed(0)," \xD7 ",i.dims().y.toFixed(0)," \xD7 ",i.dims().z.toFixed(0)," mm of ",t.pr.x," \xD7 ",t.pr.y," \xD7 ",t.pr.z),at(),Ei(t.ok?-1:5)}}function Lk(n,e){n&1&&(je(0,"p",6),it(1,"Tap a printer to show its bed and build volume. Tap it again to hide it."),ke())}function Ok(n,e){if(n&1){let t=Tc();je(0,"label",27)(1,"span",28),it(2),je(3,"b",29),it(4),ke()(),je(5,"input",30),Wn("input",function(r){Ao(t);let s=dn().$implicit,o=dn(2);return Do(o.set(s.k,r.target.valueAsNumber))}),ke()()}if(n&2){let t=dn().$implicit,i=dn(2);at(2),wi(t.label),at(2),wi(i.p()[t.k]),at(),Go("min",t.min)("max",t.max)("step",t.step)("value",i.p()[t.k])}}function Fk(n,e){if(n&1&&Mi(0,Ok,6,6,"label",27),n&2){let t=e.$implicit,i=dn().$implicit,r=dn();Ei(t.g===i&&(!t.rgb||r.p().mode==="rgb")?0:-1)}}function Uk(n,e){if(n&1){let t=Tc();je(0,"label",26)(1,"input",31),Wn("change",function(r){Ao(t);let s=dn(2);return Do(s.set("invert",r.target.checked))}),ke(),it(2," Invert (negative) "),ke()}if(n&2){let t=dn(2);at(),Go("checked",t.p().invert)}}function kk(n,e){if(n&1&&(je(0,"section",16)(1,"h2",11),it(2),ke(),Ec(3,Fk,1,1,null,null,Ak),Mi(5,Uk,3,1,"label",26),ke()),n&2){let t=e.$implicit,i=dn();at(2),wi(t),at(),wc(i.sliders),at(2),Ei(t==="Thickness"?5:-1)}}function Bk(n,e){n&1&&(je(0,"div",18),it(1,"A 60 mm bulb will not fit."),ke())}function Vk(n,e){if(n&1&&(je(0,"div",19),it(1),ke()),n&2){let t=dn();at(),zs("",t.err()," ")}}function Hk(n,e){n&1&&(je(0,"div",23)(1,"div",32)(2,"div",33),it(3,"\u{1F5BC}\uFE0F"),ke(),je(4,"div",34),it(5,"Drop a picture here"),ke(),je(6,"div",6),it(7,"your lamp appears in 3D"),ke()()())}function zk(n,e){if(n&1&&it(0),n&2){let t=e;Wd(" \xB7 ",t.x.toFixed(0)," \xD7 ",t.y.toFixed(0)," \xD7 ",t.z.toFixed(0)," mm ")}}function Gk(n,e){if(n&1&&(je(0,"div",35),it(1),Mi(2,zk,1,3),ke(),je(3,"div",36),it(4," Drag to rotate \xB7 scroll to zoom "),ke()),n&2){let t,i=dn();at(),Gd(" ",i.inner().toFixed(0)," mm inside \xB7 ",i.p().arc,"\xB0 arc"),at(),Ei((t=i.dims())?2:-1,t)}}var Wk=[{k:"width",g:"Shape",label:"Picture width (mm)",min:40,max:300,step:1},{k:"arc",g:"Shape",label:"Arc around the bulb (\xB0)",min:30,max:350,step:1},{k:"hole",g:"Shape",label:"Hole in base (mm)",min:10,max:80,step:.5},{k:"baseH",g:"Shape",label:"Base thickness (mm)",min:1,max:8,step:.5},{k:"border",g:"Shape",label:"Frame (mm)",min:0,max:8,step:.5},{k:"minT",g:"Thickness",label:"Thinnest = brightest (mm)",min:.4,max:2,step:.1},{k:"maxT",g:"Thickness",label:"Thickest = darkest (mm)",min:1.5,max:5,step:.1},{k:"gamma",g:"Thickness",label:"Midtones (gamma)",min:.5,max:2.5,step:.05},{k:"pixel",g:"Print",label:"Export detail (mm, lower = sharper)",min:.1,max:.4,step:.05},{k:"stripe",g:"Print",label:"Color band height (mm)",min:.4,max:6,step:.2,rgb:!0},{k:"layerH",g:"Print",label:"Layer height (mm)",min:.08,max:.3,step:.04,rgb:!0}],C_=[{id:"a1mini",name:"A1 mini",x:180,y:180,z:180},{id:"a1",name:"A1",x:256,y:256,z:256},{id:"p1p",name:"P1P",x:256,y:256,z:256},{id:"p1s",name:"P1S",x:256,y:256,z:256},{id:"p2s",name:"P2S",x:256,y:256,z:256},{id:"x1c",name:"X1 Carbon",x:256,y:256,z:256},{id:"x1e",name:"X1E",x:256,y:256,z:256},{id:"x2d",name:"X2D",x:256,y:256,z:260},{id:"h2s",name:"H2S",x:340,y:320,z:340},{id:"h2d",name:"H2D",x:350,y:320,z:325},{id:"h2c",name:"H2C",x:330,y:320,z:325}],$l=class n{cv;host;sliders=Wk;groups=["Shape","Thickness","Print"];printers=C_;printer=Dn(null);dims=Dn(null);fitInfo=Ic(()=>{let e=this.dims(),t=C_.find(r=>r.id===this.printer());if(!e||!t)return null;let i=[e.x>t.x?"width":"",e.y>t.y?"depth":"",e.z>t.z?"height":""].filter(Boolean);return{pr:t,ok:!i.length,over:i}});p=Dn(oe({},Ow));img=Dn(null);err=Dn("");busy=Dn(!1);inner=Ic(()=>2*(this.p().width+2*this.p().border)/(this.p().arc*Math.PI/180));r;cam;ctl;scene=new _l;mesh;bulb;world=new Ni;rulers=new Ni;bed=new Ni;pending=0;raf=0;fit=!0;ro;constructor(){sg(()=>{this.p(),this.img(),this.printer(),Yi(()=>{this.pending||(this.pending=requestAnimationFrame(()=>{this.pending=0,this.rebuild()}))})})}choose(e){this.fit=!0,this.printer.set(this.printer()===e?null:e)}set(e,t){this.p.update(i=>tt(oe({},i),{[e]:t}))}pick(e){this.load(e.target.files?.[0])}drop(e){e.preventDefault(),this.load(e.dataTransfer?.files?.[0])}load(e){if(!e||!e.type.startsWith("image/"))return;let t=new Image;t.onload=()=>{this.fit=!0,this.img.set(t)},t.src=URL.createObjectURL(e)}ngAfterViewInit(){this.r=new Ip({canvas:this.cv.nativeElement,antialias:!0,alpha:!0}),this.cam=new Mn(40,1,1,5e3),this.cam.position.set(0,90,260),this.ctl=new Np(this.cam,this.cv.nativeElement),this.bulb=new En(new Al(30,32,16),new Cr({color:16769704,transparent:!0,opacity:.9})),this.world.rotation.x=-Math.PI/2,this.world.add(this.bulb,this.rulers,this.bed),this.scene.add(this.world),this.ro=new ResizeObserver(()=>{let{clientWidth:t,clientHeight:i}=this.host.nativeElement;this.r.setSize(t,i,!1),this.cam.aspect=t/i,this.cam.updateProjectionMatrix()}),this.ro.observe(this.host.nativeElement);let e=()=>{this.ctl.update(),this.r.render(this.scene,this.cam),this.raf=requestAnimationFrame(e)};e()}ngOnDestroy(){cancelAnimationFrame(this.raf),cancelAnimationFrame(this.pending),this.ro?.disconnect(),this.r?.dispose()}rebuild(){let e=this.img();if(e)try{let t=tt(oe({},this.p()),{pixel:Math.max(this.p().pixel,this.p().width/240)}),i=T_(t,w_(e,t));this.show(i,t),this.err.set("")}catch(t){this.err.set(t.message)}}show(e,t){let i=new pn;i.setAttribute("position",new fn(e.V,3)),i.setAttribute("color",new fn(e.C,3)),i.setIndex(new fn(e.F,1)),this.mesh&&(this.world.remove(this.mesh),this.mesh.geometry.dispose(),this.mesh.material.dispose()),this.mesh=new En(i,new Cr({vertexColors:!0,side:Qn})),this.world.add(this.mesh);let r=e.box,s=r.x1-r.x0,o=r.y1-r.y0,a=r.z1,c=(r.x0+r.x1)/2,l=(r.y0+r.y1)/2,u=C_.find(d=>d.id===this.printer());if(this.dims.set({x:s,y:o,z:a}),this.bulb.position.set(0,0,t.baseH+32),this.drawRulers(r),this.drawBed(u,c,l),this.fit){let d=Math.max(s,o,a,u?Math.max(u.x,u.y,u.z)*.95:0);this.ctl.target.set(c,a/2,-l),this.cam.position.set(c,a/2+d*.6,-l+d*1.7),this.fit=!1}}drawRulers(e){this.clear(this.rulers);let t=12,i=.2,r=[],s=(h,m)=>r.push(...h,...m),o=(h,m,y)=>{s(h,m);let g=Math.hypot(m[0]-h[0],m[1]-h[1],m[2]-h[2]),p=[0,1,2].map(b=>(m[b]-h[b])/g);for(let b=0;b<=g+.01;b+=10){let A=[0,1,2].map(S=>h[S]+p[S]*b);s(A,[A[0]+y[0],A[1]+y[1],A[2]+y[2]])}s(h,[h[0]+y[0]*2.5,h[1]+y[1]*2.5,h[2]+y[2]*2.5]),s(m,[m[0]+y[0]*2.5,m[1]+y[1]*2.5,m[2]+y[2]*2.5])},{x0:a,x1:c,y0:l,y1:u,z1:d}=e;o([a,l-t,i],[c,l-t,i],[0,-2,0]),o([c+t,l,i],[c+t,u,i],[2,0,0]),o([a-t,l-t,0],[a-t,l-t,d],[-2,0,0]),s([a,l,i],[a,l-t,i]),s([c,l,i],[c,l-t,i]),s([c,l,i],[c+t,l,i]),s([c,u,i],[c+t,u,i]),s([a,l,d],[a-t,l-t,d]),this.rulers.add(this.lines(r,16498468,1));let f=Math.max(5,Math.max(c-a,u-l,d)/16);this.rulers.add(this.label(`${(c-a).toFixed(1)} mm`,[(a+c)/2,l-t-f,i],f),this.label(`${(u-l).toFixed(1)} mm`,[c+t+f*2,(l+u)/2,i],f),this.label(`${d.toFixed(1)} mm`,[a-t-f*2,l-t,d/2],f))}drawBed(e,t,i){if(this.clear(this.bed),!e)return;let{x:r,y:s,z:o}=e;this.bed.position.set(t,i,0);let a=new En(new no(r,s),new Cr({color:2763313,transparent:!0,opacity:.8,side:Qn,depthWrite:!1}));a.position.z=-.3,this.bed.add(a);let c=[],l=[];for(let f=0;f<=r;f+=10)(f%50?c:l).push(-r/2+f,-s/2,-.2,-r/2+f,s/2,-.2);for(let f=0;f<=s;f+=10)(f%50?c:l).push(-r/2,-s/2+f,-.2,r/2,-s/2+f,-.2);l.push(-r/2,-s/2,-.2,r/2,-s/2,-.2,r/2,-s/2,-.2,r/2,s/2,-.2,r/2,s/2,-.2,-r/2,s/2,-.2,-r/2,s/2,-.2,-r/2,-s/2,-.2),this.bed.add(this.lines(c,3816004,1),this.lines(l,9079446,1));let u=new Ca(new Il(new rs(r,s,o)),new to({color:7041664,transparent:!0,opacity:.5}));u.position.z=o/2,this.bed.add(u);let d=Math.max(6,r/38);for(let f=0;f*50<=Math.max(r,s,o);f++){let h=f*50;h<=r&&this.bed.add(this.label(`${h}`,[-r/2+h,-s/2-d,0],d,"#a1a1aa")),h<=s&&this.bed.add(this.label(`${h}`,[-r/2-d*1.3,-s/2+h,0],d,"#a1a1aa")),h>0&&h<=o&&this.bed.add(this.label(`${h}`,[-r/2-d*1.3,-s/2,h],d,"#a1a1aa"))}this.bed.add(this.label(`${e.name}  ${r} \xD7 ${s} \xD7 ${o} mm`,[0,-s/2,o+d],d*1.2,"#e4e4e7"))}lines(e,t,i){let r=new pn;return r.setAttribute("position",new jt(e,3)),new Ca(r,new to({color:t,transparent:i<1,opacity:i}))}label(e,t,i,r="#fbbf24"){let s=document.createElement("canvas");s.width=384,s.height=64;let o=s.getContext("2d");o.font="600 34px system-ui, sans-serif",o.textAlign="center",o.textBaseline="middle",o.fillStyle=r,o.fillText(e,192,32);let a=new Ml(new Ta({map:new Tl(s),depthTest:!1,transparent:!0}));return a.position.set(t[0],t[1],t[2]),a.scale.set(i*6,i,1),a}clear(e){e.children.slice().forEach(t=>{e.remove(t);let i=t;i.geometry?.dispose(),i.material?.map?.dispose(),i.material?.dispose()})}async download(){let e=this.img();if(e){this.busy.set(!0),await new Promise(t=>setTimeout(t,30));try{let t=this.p(),i=w_(e,t),r=T_(t,i);this.save(Fw(r),"lithophane_lamp.stl"),t.mode==="rgb"&&this.save(new Blob([Uw(t,i)],{type:"text/plain"}),"lithophane_lamp_swaps.txt"),this.err.set("")}catch(t){this.err.set(t.message)}this.busy.set(!1)}}save(e,t){let i=document.createElement("a");i.href=URL.createObjectURL(e),i.download=t,i.click(),setTimeout(()=>URL.revokeObjectURL(i.href),2e3)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Hs({type:n,selectors:[["app-lithophane-create"]],viewQuery:function(t,i){if(t&1&&Vd(Tk,5)(Ck,5),t&2){let r;Hd(r=zd())&&(i.cv=r.first),Hd(r=zd())&&(i.host=r.first)}},hostAttrs:[1,"flex","h-screen","bg-zinc-950","text-zinc-100","font-sans"],decls:58,vars:20,consts:[["host",""],["cv",""],[1,"w-80","shrink-0","space-y-6","overflow-y-auto","border-r","border-white/10","bg-zinc-900","p-5"],[1,"flex","items-center","gap-3"],[1,"grid","h-10","w-10","place-items-center","rounded-xl","bg-amber-400","text-xl","shadow-lg","shadow-amber-400/20"],[1,"text-base","font-semibold","leading-tight"],[1,"text-xs","text-zinc-500"],[1,"flex","cursor-pointer","flex-col","items-center","gap-1","rounded-xl","border","border-dashed","border-white/20","py-5","text-sm","text-zinc-300","transition","hover:border-amber-400","hover:bg-white/5"],[1,"font-medium"],["type","file","accept","image/*","hidden","",3,"change"],[1,"space-y-2"],[1,"text-xs","font-semibold","uppercase","tracking-wider","text-zinc-500"],[1,"grid","grid-cols-2","rounded-xl","bg-zinc-800","p-1"],[3,"click"],[1,"grid","grid-cols-3","gap-1.5"],[3,"class"],[1,"space-y-3"],[1,"font-mono"],[1,"mt-1","text-xs"],[1,"rounded-xl","border","border-red-400/40","bg-red-400/10","p-3","text-xs","text-red-200"],[1,"w-full","rounded-xl","bg-emerald-500","py-3","font-semibold","text-zinc-950","shadow-lg","shadow-emerald-500/20","transition","hover:bg-emerald-400","disabled:cursor-not-allowed","disabled:opacity-40",3,"click","disabled"],[1,"relative","min-w-0","flex-1","bg-gradient-to-b","from-zinc-800","via-zinc-900","to-black",3,"dragover","drop"],[1,"block","h-full","w-full"],[1,"pointer-events-none","absolute","inset-0","grid","place-items-center"],[1,"font-semibold"],[1,"mt-1"],[1,"flex","items-center","gap-2","text-sm","text-zinc-300"],[1,"block"],[1,"flex","justify-between","text-xs","text-zinc-400"],[1,"font-mono","text-zinc-100"],["type","range",1,"mt-1","w-full","accent-amber-400",3,"input","min","max","step","value"],["type","checkbox",1,"size-4","accent-amber-400",3,"change","checked"],[1,"rounded-2xl","border","border-dashed","border-white/20","px-10","py-8","text-center","text-zinc-400"],[1,"text-3xl"],[1,"mt-2","font-medium"],[1,"absolute","right-4","top-4","rounded-full","border","border-white/10","bg-black/50","px-3","py-1","text-xs","text-zinc-300","backdrop-blur"],[1,"pointer-events-none","absolute","bottom-4","left-1/2","-translate-x-1/2","rounded-full","bg-black/50","px-3","py-1","text-xs","text-zinc-400","backdrop-blur"]],template:function(t,i){if(t&1&&(je(0,"aside",2)(1,"header",3)(2,"div",4),it(3,"\u{1F4A1} "),ke(),je(4,"div")(5,"h1",5),it(6,"Lithophane Lamp"),ke(),je(7,"p",6),it(8,"Picture, curve, base, STL"),ke()()(),je(9,"label",7)(10,"span",8),it(11),ke(),je(12,"span",6),it(13,"or drop it on the preview"),ke(),je(14,"input",9),Wn("change",function(s){return i.pick(s)}),ke()(),je(15,"section",10)(16,"h2",11),it(17,"Color mode"),ke(),je(18,"div",12)(19,"button",13),Wn("click",function(){return i.set("mode","single")}),it(20,"Single color"),ke(),je(21,"button",13),Wn("click",function(){return i.set("mode","rgb")}),it(22,"RGB swaps"),ke()(),je(23,"p",6),it(24),ke()(),je(25,"section",10)(26,"h2",11),it(27,"Base shape"),ke(),je(28,"div",12)(29,"button",13),Wn("click",function(){return i.set("baseShape","semi")}),it(30,"Semicircle"),ke(),je(31,"button",13),Wn("click",function(){return i.set("baseShape","full")}),it(32,"Full circle"),ke()(),Mi(33,Dk,2,0,"p",6),ke(),je(34,"section",10)(35,"h2",11),it(36,"Printer bed"),ke(),je(37,"div",14),Ec(38,Rk,2,3,"button",15,Ik),ke(),Mi(40,Pk,6,10,"div",15)(41,Lk,2,0,"p",6),ke(),Ec(42,kk,6,2,"section",16,hv),je(44,"div"),it(45," Inside diameter "),je(46,"b",17),it(47),ke(),Mi(48,Bk,2,0,"div",18),ke(),Mi(49,Vk,2,1,"div",19),je(50,"button",20),Wn("click",function(){return i.download()}),it(51),ke()(),je(52,"main",21,0),Wn("dragover",function(s){return s.preventDefault()})("drop",function(s){return i.drop(s)}),Bd(54,"canvas",22,1),Mi(56,Hk,8,0,"div",23)(57,Gk,5,3),ke()),t&2){let r;at(11),wi(i.img()?"Change picture":"Choose a picture"),at(8),mr("rounded-lg py-2 text-sm font-medium transition "+(i.p().mode==="single"?"bg-amber-400 text-zinc-900 shadow":"text-zinc-400 hover:text-white")),at(2),mr("rounded-lg py-2 text-sm font-medium transition "+(i.p().mode==="rgb"?"bg-amber-400 text-zinc-900 shadow":"text-zinc-400 hover:text-white")),at(3),wi(i.p().mode==="rgb"?"Prints in red, green and blue bands. You also get a swap sheet.":"One filament, thickness makes the picture."),at(5),mr("rounded-lg py-2 text-sm font-medium transition "+(i.p().baseShape==="semi"?"bg-amber-400 text-zinc-900 shadow":"text-zinc-400 hover:text-white")),at(2),mr("rounded-lg py-2 text-sm font-medium transition "+(i.p().baseShape==="full"?"bg-amber-400 text-zinc-900 shadow":"text-zinc-400 hover:text-white")),at(2),Ei(i.p().baseShape==="semi"?33:-1),at(5),wc(i.printers),at(2),Ei((r=i.fitInfo())?40:41,r),at(2),wc(i.groups),at(2),mr("rounded-xl border p-3 text-sm "+(i.inner()<66?"border-orange-400/40 bg-orange-400/10 text-orange-200":"border-white/10 bg-zinc-800 text-zinc-300")),at(3),zs("",i.inner().toFixed(0)," mm"),at(),Ei(i.inner()<66?48:-1),at(),Ei(i.err()?49:-1),at(),Go("disabled",!i.img()||i.busy()),at(),wi(i.busy()?"Working\u2026":"Download STL"),at(5),Ei(i.img()?57:56)}},styles:["[_nghost-%COMP%]{display:flex}"]})};var kw=[{path:"",component:$l},{path:"lithophane-create",component:$l}];var Bw={providers:[ig(),ty(kw)]};var Pp=class n{title=Dn("lithophane-ui");static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Hs({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&zo(0,"router-outlet")},dependencies:[Kc],encapsulation:2})};Cv(Pp,Bw).catch(n=>console.error(n));
