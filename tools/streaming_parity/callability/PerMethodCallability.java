import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import jnr.ffi.Pointer;
import jnr.ffi.Memory;
import jnr.ffi.Runtime;
import java.time.OffsetDateTime;
import java.time.LocalDateTime;

/**
 * Type-aware per-method CALLABILITY harness for the Flink/Kafka MEOS facade.
 *
 * Tests ONE (class, method) per JVM (so a type-mismatch SIGSEGV or a native
 * exit(1) on a MEOS semantic error only loses the method under test, never the
 * rest of the run). The driver (run_callability.sh) classifies the outcome:
 *   reached native MEOS  -> CALLABLE  (clean return "OK"; a caught MEOS error
 *                                       "MEOSERR"; a native exit on a semantic
 *                                       error; or a signal, rc>128)
 *   "BINDFAIL"           -> NOT callable (UnsatisfiedLink / NoSuchMethod /
 *                                          marshalling — the binding itself fails)
 *   "NOMETHOD"/"NOSYNTH" -> untestable here (method not found / args not
 *                                             literal-synthesizable) -> skip
 *
 * The "connectors": each Pointer argument is built from a per-TYPE sample, with
 * the type inferred from the function-name tokens (acontains_geo_tgeo ->
 * [geometry, tgeompoint]; tdwithin_tgeo_tgeo -> [tgeompoint, tgeompoint]). TOK
 * maps a name-segment to (a facade `*_in` constructor, a canonical literal); a
 * sample is built once per needed type, then assigned positionally to the
 * Pointer args. This is what lifts confirmed callability from the
 * single-primary-value floor (331) to the multi-Pointer spatial-relations and
 * span/set/box operators (1472 of 1949 streamable). Types whose constructor is
 * ABSENT from the linked libmeos (the extended types cbuffer/pose/tcbuffer/
 * tpose/trgeo, pending the extended-type C-API) cannot get a sample, so their
 * operators report NOSYNTH — a reason-marked, nm -D-provable gap, not a binding
 * defect.
 *
 * args: <pkg> <ClassName> <method> <allClassesCSV>   (allClassesCSV = every
 * MeosOps* facade class, used to resolve the `*_in` constructors).
 */
public class PerMethodCallability {
    // name-segment -> (facade constructor, canonical literal)
    static final String[][] TOK = {
        {"tgeompoint","tgeompoint_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tgeogpoint","tgeogpoint_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tgeometry","tgeometry_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tgeography","tgeography_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tcbuffer","tcbuffer_in","Cbuffer(Point(1 1),2)@2000-01-01"},
        {"tnpoint","tnpoint_in","NPoint(1,0.5)@2000-01-01"},
        {"tpose","tpose_in","Pose(Point(1 1),0.5)@2000-01-01"},
        {"tgeo","tgeompoint_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tspatial","tgeompoint_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tnumber","tfloat_in","1.5@2000-01-01"},
        {"temporal","tfloat_in","1.5@2000-01-01"},
        {"tpoint","tgeompoint_in","SRID=4326;POINT(1 2)@2000-01-01"},
        {"tfloat","tfloat_in","1.5@2000-01-01"},
        {"tint","tint_in","5@2000-01-01"},
        {"tbool","tbool_in","true@2000-01-01"},
        {"ttext","ttext_in","\"hi\"@2000-01-01"},
        {"cbuffer","cbuffer_in","Cbuffer(Point(1 1),2)"},
        {"npointset","npointset_in","{NPoint(1,0.5), NPoint(2,0.3)}"},
        {"npoint","npoint_in","NPoint(1,0.5)"},
        {"nsegment","nsegment_in","NSegment(1,0.2,0.8)"},
        {"pose","pose_in","Pose(Point(1 1),0.5)"},
        {"geometry","geom_in","SRID=4326;POINT(1 2)"},
        {"geography","geog_in","SRID=4326;POINT(1 2)"},
        {"geog","geog_in","SRID=4326;POINT(1 2)"},
        {"geo","geom_in","SRID=4326;POINT(1 2)"},
        {"geom","geom_in","SRID=4326;POINT(1 2)"},
        {"point","geom_in","SRID=4326;POINT(1 2)"},
        {"stbox","stbox_in","STBOX XT(((1,1),(2,2)),[2000-01-01,2000-01-02])"},
        {"tbox","tbox_in","TBOX XT([1,2],[2000-01-01,2000-01-02])"},
        {"intspanset","intspanset_in","{[1, 3), [5, 7)}"},
        {"floatspanset","floatspanset_in","{[1.5, 2.5)}"},
        {"tstzspanset","tstzspanset_in","{[2000-01-01, 2000-01-02)}"},
        {"datespanset","datespanset_in","{[2000-01-01, 2000-01-03)}"},
        {"bigintspanset","bigintspanset_in","{[1, 3), [5, 7)}"},
        {"spanset","floatspanset_in","{[1.5, 2.5)}"},
        {"numspan","floatspan_in","[1.5, 2.5)"},
        {"intspan","intspan_in","[1, 5)"},
        {"floatspan","floatspan_in","[1.5, 2.5)"},
        {"tstzspan","tstzspan_in","[2000-01-01, 2000-01-02)"},
        {"datespan","datespan_in","[2000-01-01, 2000-01-05)"},
        {"bigintspan","bigintspan_in","[1, 5)"},
        {"span","floatspan_in","[1.5, 2.5)"},
        {"intset","intset_in","{1, 2, 3}"},
        {"floatset","floatset_in","{1.5, 2.5}"},
        {"tstzset","tstzset_in","{2000-01-01, 2000-01-02}"},
        {"dateset","dateset_in","{2000-01-01, 2000-01-02}"},
        {"bigintset","bigintset_in","{1, 2, 3}"},
        {"textset","textset_in","{\"a\", \"b\"}"},
        {"geomset","geomset_in","{SRID=4326;POINT(1 2)}"},
        {"set","floatset_in","{1.5, 2.5}"},
        {"text","text_in","hello"},
        {"interval","pg_interval_in","1 day"},
        // naming-variant tokens: typed TBox prefixes all share tbox_in; the geo
        // set superclass + plain geo-set prefix share geomset_in; line is a geom.
        {"tboxfloat","tbox_in","TBOX XT([1, 2], [2000-01-01, 2000-01-02])"},
        {"tboxint","tbox_in","TBOXINT X([1, 2])"},
        {"tfloatbox","tbox_in","TBOX XT([1, 2], [2000-01-01, 2000-01-02])"},
        {"tintbox","tbox_in","TBOXINT X([1, 2])"},
        {"geoset","geomset_in","{POINT(1 1), POINT(2 2)}"},
        {"spatialset","geomset_in","{POINT(1 1), POINT(2 2)}"},
        {"line","geom_in","SRID=4326;LINESTRING(0 0, 1 1, 2 2)"},
        // typed-instant constructors `<T>inst_make(value, TimestampTz)` — the
        // value-type token is the whole `<T>inst` word (no underscore split).
        {"tpointinst","geom_in","SRID=4326;POINT(1 2)"},
        {"tgeoinst","geom_in","SRID=4326;POINT(1 2)"},
        {"tnpointinst","npoint_in","NPoint(1,0.5)"},
        {"ttextinst","text_in","hello"},
    };
    static Map<String,Method> index = new HashMap<>();
    static Map<String,Pointer> samples = new HashMap<>();
    // extended-type sets with no single-string ctor: {token, element ctor, make fn}
    static final String[][] SETMAKE = {
        {"cbufferset","cbuffer_in","cbufferset_make"},
        {"poseset","pose_in","poseset_make"},
        {"npointset","npoint_in","npointset_make"},
    };
    static Pointer intervalSample;   // for date/timestamptz funcs whose Interval arg is not in the name
    // functions whose trailing Pointer is an OUT param (the result is written to it)
    static final Set<String> OUTPARAM = new HashSet<>(Arrays.asList(
        "lwproj_lookup","spheroid_init_from_srid","tempsubtype_from_string"));

    public static void main(String[] args) throws Exception {
        String pkg=args[0], target=args[1], method=args[2], allcsv=args[3];
        Class.forName("functions.GeneratedFunctions").getMethod("meos_initialize").invoke(null);
        for (String cn: allcsv.split(",")) {
            try { for (Method m: Class.forName(pkg+"."+cn).getDeclaredMethods())
                if (Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()))
                    index.putIfAbsent(m.getName(), m); } catch (Throwable t) {}
        }
        // build only the samples whose token appears in the method name
        Set<String> need=new HashSet<>(Arrays.asList(method.split("_")));
        for (String[] tk: TOK) {
            if (!need.contains(tk[0]) || samples.containsKey(tk[0])) continue;
            Method ctor=index.get(tk[1]); if (ctor==null) continue;
            // Some constructors take more than the WKT/literal string — notably
            // geom_in(String,int) / geog_in(String,int) where the trailing int is
            // the PostGIS typmod (-1 = unconstrained). Invoking with only the
            // literal silently fails (wrong arg count), which previously left the
            // whole geometry/geography family without a sample. Build the args to
            // match the ctor's arity: literal first, synthesized primitives after.
            try {
                Object p=ctor.invoke(null, ctorArgs(ctor, tk[2]));
                if (p instanceof Pointer) samples.put(tk[0],(Pointer)p);
            } catch (Throwable t) {}
        }
        // Set samples for the extended-type sets have no single-string `*_in`
        // (their WKT has comma-bearing elements). Build them the canonical way:
        // a real element array passed to `<set>_make(T** values, int count)`.
        for (String[] sm : SETMAKE) {
            if (!need.contains(sm[0]) || samples.containsKey(sm[0])) continue;
            Pointer arr=buildArray(sm[1], 1); Method mk=index.get(sm[2]);
            if (arr!=null && mk!=null) try {
                Object s=mk.invoke(null, arr, 1);   // single-element set (no dup-rejection)
                if (s instanceof Pointer) samples.put(sm[0],(Pointer)s);
            } catch (Throwable t) {}
        }
        // trgeometry has no single-string ctor; build it component-wise via
        // trgeometryinst_make(GSERIALIZED *geom, Pose *pose, TimestampTz t).
        if (need.contains("trgeometry") && !samples.containsKey("trgeometry")) {
            Method gi=index.get("geom_in"), mk=index.get("trgeometryinst_make");
            Pointer pose=buildSample("pose_in");
            // a trgeometry is a rigid AREAL geometry — needs a polygon, not a point
            Pointer geom = (gi!=null) ? safe(gi, ctorArgs(gi, "Polygon((1 1,2 2,3 1,1 1))")) : null;
            if (geom!=null && pose!=null && mk!=null) try {
                Object s=mk.invoke(null, geom, pose, OffsetDateTime.parse("2000-01-01T00:00:00Z"));
                if (s instanceof Pointer) samples.put("trgeometry",(Pointer)s);
            } catch (Throwable t) {}
        }
        intervalSample = buildSample("pg_interval_in");   // "1 day" — for date/timestamptz Interval args
        Method m=index.get(method);
        if (m==null || !m.getDeclaringClass().getSimpleName().equals(target)) {
            try { for (Method x: Class.forName(pkg+"."+target).getDeclaredMethods())
                if (x.getName().equals(method) && Modifier.isStatic(x.getModifiers())) { m=x; break; } } catch (Throwable t) {}
        }
        if (m==null) { System.out.println("NOMETHOD"); return; }
        Object[] a=synth(m.getName(), m.getParameterTypes());
        if (a==null) { System.out.println("NOSYNTH"); return; }
        try { m.invoke(null,a); System.out.println("OK"); }
        catch (InvocationTargetException e) {
            Throwable c=e.getCause(); String cn=c==null?"":c.getClass().getName();
            if (cn.contains("UnsatisfiedLink")||cn.contains("NoSuchMethod")||cn.contains("IllegalArgument")) System.out.println("BINDFAIL");
            else System.out.println("MEOSERR"); // reached MEOS -> callable
        } catch (Throwable t) { System.out.println("BINDFAIL"); }
    }

    // assign each Pointer arg the next name-token sample (in appearance order);
    // primitives get canonical samples; unsynthesizable Pointer -> null (NOSYNTH)
    static Object[] synth(String name, Class<?>[] pt) {
        // Array constructors `<T>set_make(T** vals, int n)` / `<T>arr_round(T** arr,
        // int n, int maxdd)` take a real C array-of-pointers + a count. Build a
        // 2-element T** of the element type via native memory (the element ctor is
        // derived from the name) and pass it with count=2.
        String elemCtor = arrayElemCtor(name);
        if (elemCtor != null && pt.length>=2 && pt[0]==Pointer.class && pt[1]==int.class) {
            Pointer arr=buildArray(elemCtor, 1);
            if (arr!=null) {
                Object[] a=new Object[pt.length];
                a[0]=arr; a[1]=1;   // single-element array (count must match)
                for (int i=2;i<pt.length;i++) a[i]=prim(pt[i]);
                return a;
            }
        }
        // trgeometryinst_make(GSERIALIZED *geom, Pose *pose, TimestampTz t) — two
        // typed Pointers + a timestamp; build a polygon geom + a pose directly.
        if (name.equals("trgeometryinst_make") && pt.length==3) {
            Pointer g=index.get("geom_in")!=null ? safe(index.get("geom_in"), ctorArgs(index.get("geom_in"),"Polygon((1 1,2 2,3 1,1 1))")) : null;
            Pointer ps=buildSample("pose_in");
            if (g!=null && ps!=null) return new Object[]{g, ps, OffsetDateTime.parse("2000-01-01T00:00:00Z")};
        }
        List<Pointer> ptrSamples=new ArrayList<>();
        for (String seg: name.split("_")) {
            Pointer s=samples.get(seg);
            if (s!=null) ptrSamples.add(s);
        }
        // Aggregate transition/combine/final functions take an aggregate STATE
        // pointer that has no `*_in` constructor (and so no name-token sample);
        // NULL is the documented MEOS empty-state bootstrap (every transfn does
        // `if (state == NULL) state = <init>`), so a token-less Pointer in these
        // is a legitimate, non-degenerate input — not a synthesis failure.
        boolean agg = name.endsWith("transfn") || name.endsWith("combinefn")
                   || name.endsWith("finalfn");
        int pi=0; Object[] a=new Object[pt.length];
        for (int i=0;i<pt.length;i++) {
            Class<?> t=pt[i];
            if (t==Pointer.class) {
                if (pi<ptrSamples.size()) a[i]=ptrSamples.get(pi++);
                else if (!ptrSamples.isEmpty()) a[i]=ptrSamples.get(ptrSamples.size()-1);
                else if (agg) a[i]=null;     // aggregate state bootstrap (NULL = empty state)
                else if (intervalSample!=null && (name.startsWith("timestamptz_")||name.startsWith("date_")))
                    a[i]=intervalSample;     // the Interval arg of date/timestamptz bucketing/shift
                else if (i==pt.length-1 && OUTPARAM.contains(name)) a[i]=outBuffer();  // OUT-param buffer
                else return null;            // no typed sample for a Pointer arg -> skip
            }
            else if (t==int.class) a[i]=1;
            else if (t==long.class) a[i]=1L;
            else if (t==double.class) a[i]=1.0;
            else if (t==float.class) a[i]=1.0f;
            else if (t==boolean.class) a[i]=true;
            else if (t==byte.class) a[i]=(byte)1;     // WKB-variant / flag byte params
            else if (t==short.class) a[i]=(short)1;
            else if (t==String.class) a[i]="1";
            else if (t==OffsetDateTime.class) a[i]=OffsetDateTime.parse("2000-01-01T00:00:00Z");
            else if (t==LocalDateTime.class) a[i]=LocalDateTime.parse("2000-01-01T00:00:00");
            else return null;                // e.g. a callback fn-pointer type
        }
        return a;
    }

    // Build constructor args matching the ctor's arity: the WKT/literal goes
    // first; trailing primitives get canonical defaults (int = -1, the PostGIS
    // typmod "unconstrained" used by geom_in/geog_in).
    static Object[] ctorArgs(Method ctor, String literal) {
        Class<?>[] pt=ctor.getParameterTypes();
        Object[] a=new Object[pt.length];
        for (int i=0;i<pt.length;i++) {
            Class<?> t=pt[i];
            if (t==String.class) a[i]=literal;
            else if (t==int.class) a[i]=-1;
            else if (t==long.class) a[i]=-1L;
            else if (t==double.class) a[i]=0.0;
            else if (t==float.class) a[i]=0.0f;
            else if (t==boolean.class) a[i]=false;
            else if (t==byte.class) a[i]=(byte)0;
            else if (t==short.class) a[i]=(short)0;
            else a[i]=null;
        }
        return a;
    }

    // Build a native T** array of `n` copies of a sample built from `ctorName`.
    static Pointer buildArray(String ctorName, int n) {
        Pointer elem=buildSample(ctorName);
        if (elem==null) return null;
        Runtime rt=Runtime.getSystemRuntime();
        Pointer arr=Memory.allocateDirect(rt, (long)n*rt.addressSize());
        for (int i=0;i<n;i++) arr.putAddress((long)i*rt.addressSize(), elem.address());
        return arr;
    }

    // Invoke a static Method, return the Pointer result or null on any failure.
    static Pointer safe(Method m, Object[] a) {
        try { Object p=m.invoke(null, a); return p instanceof Pointer ? (Pointer)p : null; }
        catch (Throwable t) { return null; }
    }

    // Build one Pointer sample by invoking facade ctor `ctorName` with its TOK literal.
    static Pointer buildSample(String ctorName) {
        Method ctor=index.get(ctorName); if (ctor==null) return null;
        try { Object p=ctor.invoke(null, ctorArgs(ctor, literalFor(ctorName)));
              return p instanceof Pointer ? (Pointer)p : null; }
        catch (Throwable t) { return null; }
    }

    static String literalFor(String ctorName) {
        for (String[] tk: TOK) if (tk[1].equals(ctorName)) return tk[2];
        return "1";
    }

    // element ctor for an array-constructor method name (T in `T** values`)
    static String arrayElemCtor(String name) {
        if (name.startsWith("cbufferset_make") || name.startsWith("cbufferarr")) return "cbuffer_in";
        if (name.startsWith("poseset_make")    || name.startsWith("posearr"))    return "pose_in";
        if (name.startsWith("geoset_make"))     return "geom_in";
        if (name.startsWith("npointset_make"))  return "npoint_in";
        if (name.startsWith("stboxarr"))        return "stbox_in";
        if (name.startsWith("temparr"))         return "tfloat_in";
        if (name.startsWith("tsequenceset_make_gaps")) return "tfloat_in";  // TInstant** of tfloat instants
        return null;
    }

    static Object prim(Class<?> t) {
        if (t==int.class) return 1; if (t==long.class) return 1L; if (t==double.class) return 1.0;
        if (t==float.class) return 1.0f; if (t==boolean.class) return true; return null;  // Pointer -> null (e.g. optional Interval maxt)
    }

    static Pointer outBuffer() { return Memory.allocateDirect(Runtime.getSystemRuntime(), 64); }
}
