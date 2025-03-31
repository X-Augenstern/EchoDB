package xzzzz.xz.echodb.client;

import xzzzz.xz.echodb.transport.Package;
import xzzzz.xz.echodb.transport.Packager;

public class Client {

    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if (resPkg.getErr() != null)
            throw resPkg.getErr();
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception ignored) {

        }
    }
}
