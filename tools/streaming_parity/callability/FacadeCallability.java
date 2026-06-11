import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import jnr.ffi.Pointer;

/**
 * CALLABILITY test for the MobilityFlink / MobilityKafka MEOS facade (the
 * shared JNR-FFI `MeosOps*.java` surface generated over JMEOS).
 *
 * Verifies the BINDING CONNECTION per function — that the platform can invoke
 * the MEOS function on real libmeos (symbol resolves, args marshal in, result
 * marshals back) — NOT that the result is correct (correctness is inherited
 * from MEOS's own PostgreSQL regression suite, which executes the same C code).
 *
 * Per MeosOps<Group> class: build the group's primary value from a canonical
 * literal, then invoke every public-static method (Pointer args <- the primary
 * value, primitives <- samples, String <- the literal). A method that returns
 * without a binding failure is CALLABLE; it is printed (append+flush) to the
 * output file. ONE class per JVM (crash isolation): a MEOS error on a synthetic
 * input can abort the JVM — that abort still means the call reached MEOS
 * (callable), and per-class isolation preserves every result flushed before it.
 *
 * args: <pkg> <ClassName> <outFile-append>
 */
public class FacadeCallability {
    static final Map<String,String> LIT = new HashMap<>();
    static {
        LIT.put("bigintset","{1, 2, 3}"); LIT.put("bigintspan","[1, 5)"); LIT.put("bigintspanset","{[1, 3), [5, 7)}");
        LIT.put("intset","{1, 2, 3}"); LIT.put("intspan","[1, 5)"); LIT.put("intspanset","{[1, 3), [5, 7)}");
        LIT.put("floatset","{1.5, 2.5}"); LIT.put("floatspan","[1.5, 2.5)"); LIT.put("floatspanset","{[1.5, 2.5)}");
        LIT.put("dateset","{2000-01-01, 2000-01-02}"); LIT.put("datespan","[2000-01-01, 2000-01-05)"); LIT.put("datespanset","{[2000-01-01, 2000-01-03)}");
        LIT.put("tstzset","{2000-01-01, 2000-01-02}"); LIT.put("tstzspan","[2000-01-01, 2000-01-02)"); LIT.put("tstzspanset","{[2000-01-01, 2000-01-02)}");
        LIT.put("textset","{\"a\", \"b\"}"); LIT.put("text","hello");
        LIT.put("tint","5@2000-01-01"); LIT.put("tfloat","1.5@2000-01-01"); LIT.put("tbool","true@2000-01-01"); LIT.put("ttext","\"hi\"@2000-01-01");
        LIT.put("tgeompoint","SRID=4326;POINT(1 2)@2000-01-01"); LIT.put("tgeogpoint","SRID=4326;POINT(1 2)@2000-01-01");
        LIT.put("stbox","STBOX XT(((1,1),(2,2)),[2000-01-01,2000-01-02])"); LIT.put("tbox","TBOX XT([1,2],[2000-01-01,2000-01-02])");
        LIT.put("cbuffer","Cbuffer(Point(1 1), 2)"); LIT.put("cbufferset","{Cbuffer(Point(1 1), 2)}");
        LIT.put("npoint","NPoint(1, 0.5)"); LIT.put("npointset","{NPoint(1, 0.5)}"); LIT.put("nsegment","NSegment(1, 0.2, 0.8)");
        LIT.put("tnpoint","NPoint(1, 0.5)@2000-01-01");
        LIT.put("pose","Pose(Point(1 1), 0.5)"); LIT.put("poseset","{Pose(Point(1 1), 0.5)}");
        LIT.put("geometry","SRID=4326;POINT(1 2)"); LIT.put("geography","SRID=4326;POINT(1 2)");
    }
    static String classPrimary(String cls) { return cls.substring("MeosOps".length()).toLowerCase(); }

    public static void main(String[] args) throws Exception {
        String pkg = args[0], className = args[1];
        PrintWriter w = new PrintWriter(new FileWriter(args[2], true), true);
        Class.forName("functions.GeneratedFunctions").getMethod("meos_initialize").invoke(null);
        if (className.startsWith("MeosOpsFree") || className.equals("MeosOpsRuntime")) return;
        Class<?> c = Class.forName(pkg + "." + className);
        String prim = classPrimary(className), lit = LIT.get(prim);
        Pointer primary = null;
        if (lit != null) try {
            Method in = c.getMethod(prim + "_in", String.class);
            primary = (Pointer) in.invoke(null, lit);
            if (primary != null) w.println(prim + "_in");
        } catch (Throwable t) {}
        for (Method m : c.getDeclaredMethods()) {
            if (!Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers())) continue;
            Object[] a = synth(m.getParameterTypes(), primary, lit);
            if (a == null) continue;             // can't synthesize args -> leave wired-only
            try { m.invoke(null, a); w.println(m.getName()); w.flush(); } catch (Throwable t) {}
        }
        w.close();
    }
    static Object[] synth(Class<?>[] pt, Pointer primary, String lit) {
        Object[] a = new Object[pt.length];
        for (int i = 0; i < pt.length; i++) {
            Class<?> t = pt[i];
            if (t == Pointer.class) { if (primary == null) return null; a[i] = primary; }
            else if (t == int.class) a[i] = 1;
            else if (t == long.class) a[i] = 1L;
            else if (t == double.class) a[i] = 1.0;
            else if (t == float.class) a[i] = 1.0f;
            else if (t == boolean.class) a[i] = true;
            else if (t == String.class) a[i] = (lit != null ? lit : "1");
            else return null;
        }
        return a;
    }
}
