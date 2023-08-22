/**
 * Copyright 2023, Richard Rodgers
 * SPDX-Licence-Identifier: Apache-2.0
 */

package org.modrepo.packr;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.modrepo.packr.Bag.*;
import static org.modrepo.packr.Bag.MetadataName.*;
import static org.modrepo.packr.BagBuilder.EolRule.*;

/**
 * BagBuilder is a builder class used to construct bags conformant to IETF Bagit spec - version 1.0.
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */

public class BagBuilder {

    // directory root of bag
    private final Path base;
    // checksum algorithms
    private final Set<String> csAlgs;
    // Charset encoding for tag files
    private final Charset tagEncoding;
    // line separator used by FlatWriters
    private final String lineSeparator;
    // ephemeral bag?
    private final boolean ephemeral;
    // manifest writers
    private final Map<String, FlatWriter> tagWriters = new HashMap<>();
    private final Map<String, FlatWriter> manWriters = new HashMap<>();
    // optional flat writers
    private final Map<String, FlatWriter> writers = new HashMap<>();
    // optional bag stream
    private final Map<String, BagOutputStream> streams = new HashMap<>();
    // automatic metadata generation set
    private Set<MetadataName> autogenNames = Set.of(BAGGING_DATE, BAG_SIZE, PAYLOAD_OXUM, BAG_SOFTWARE_AGENT);
    // total payload size
    private long payloadSize = 0L;
    // number of payload files
    private int payloadCount = 0;
    // has bag been built?
    private boolean built;

    /**
     * Rule for assigning the EOL (line termination/separation)
     */
    public enum EolRule {
        /**
         * Use system-defined separator
         */
        SYSTEM,
        /**
         * Use Windows separators on Unix systems,
         * or vice versa
         */
        COUNTER_SYSTEM,
        /**
         * Use Unix new-line '\n' separators
         */
        UNIX,
        /**
         * Use Windows CR/LF separators
         */
        WINDOWS,
        /**
         * Use old Mac '\r' separators
         */
        CLASSIC
    }

    /**
     * Returns a new bag builder instance using
     * a temporary directory to hold an ephemeral bag with
     * default tag encoding (UTF-8), system-defined line separator,
     * and default checksum algorithm (SHA-512)
     *
     * @throws IOException if error creating bag
     */
    public BagBuilder() throws IOException {
        this(Files.createTempDirectory("bag"), StandardCharsets.UTF_8, SYSTEM, true, DEFAULT_CS_ALGO);
    }

    /**
     * Returns a new bag builder instance using passed
     * directory to hold a non-ephemeral bag with
     * default tag encoding (UTF-8), system-defined line separator
     * and default checksum algorithm (SHA-512)
     *
     * @param base the base directory in which to construct the bag
     * @throws IOException if error creating bag
     */
    public BagBuilder(Path base) throws IOException {
        this(base, StandardCharsets.UTF_8, SYSTEM, false, DEFAULT_CS_ALGO);
    }

    /**
     * Returns a new bag builder instance using passed
     * directory to hold a non-ephemeral bag with
     * default tag encoding (UTF-8), system-defined line separator
     * and passed list of checksum algorithms (may be one)
     *
     * @param base the base directory in which to construct the bag
     * @param csAlgorithms list of checksum algorithms (may be one)
     * @throws IOException if error creating bag
     */
    public BagBuilder(Path base, String ... csAlgorithms) throws IOException {
        this(base, StandardCharsets.UTF_8, SYSTEM, false, csAlgorithms);
    }

    /**
     * Returns a new bag builder instance using passed
     * directory to hold a non-ephemeral bag with
     * passed tag encoding, system-defined line separator
     * and passed list of checksum algorithms (may be one)
     *
     * @param base the base directory in which to construct the bag
     * @param encoding tag encoding (currently UTF-8, UTF-16)
     * @param csAlgorithms list of checksum algorithms (may be one)
     * @throws IOException if error creating bag
     */
    public BagBuilder(Path base, Charset encoding, String ... csAlgorithms) throws IOException {
        this(base, encoding, SYSTEM, false, csAlgorithms);
    }

    /**
     * Returns a new bag builder instance using passed directory,
     * tag encoding, and line separator for text files, transience rule,
     * and checksum algorithms
     *
     * @param base the base directory in which to construct the bag
     * @param encoding character encoding to use for tag files
     * @param eolRule line termination rule to use for generated text files. Values are:
     *            SYSTEM - use system-defined line termination
     *            COUNTER_SYSTEM - if on Windows, use Unix EOL, else reverse
     *            UNIX - use newline character line termination
     *            WINDOWS - use CR/LF line termination
     *            CLASSIC - use CR line termination
     * @param ephemeral if true, remove after reading network stream closes
     * @param csAlgorithms list of checksum algorithms (may be one)
     *
     * @throws IOException if error creating bag
     */
    public BagBuilder(Path base, Charset encoding, EolRule eolRule, boolean ephemeral, String ... csAlgorithms) throws IOException {
        this.base = base;
        tagEncoding = encoding;
        this.ephemeral = ephemeral;
        csAlgs = Set.of(csAlgorithms);
        Path dirPath = bagFile(DATA_DIR);
        if (Files.notExists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        // verify these are legit and fail if not
        if (csAlgs.isEmpty()) {
            throw new IOException("No checksum algorithm specified");
        }
        for (String alg : csAlgs) {
            try {
                MessageDigest.getInstance(alg);
            } catch (NoSuchAlgorithmException nsaE) {
                throw new IOException("No such checksum algorithm: " + alg);
            }
            addWriters(alg);
        }
        String sysEol = System.lineSeparator();
        lineSeparator = switch (eolRule) {
            case SYSTEM -> sysEol;
            case UNIX -> "\n";
            case WINDOWS -> "\r\n";
            case CLASSIC -> "\r"; 
            case COUNTER_SYSTEM -> "\n".equals(sysEol) ? "\r\n" : "\n";
            default -> sysEol;
        };
    }

    private void addWriters(String csAlgorithm) throws IOException {
        var sfx = csAlgoName(csAlgorithm) + ".txt";
        var tagWriter = new FlatWriter(bagFile(TAGMANIF_FILE + sfx), null, null, false, tagEncoding);
        tagWriters.put(csAlgorithm, tagWriter);
        manWriters.put(csAlgorithm, new FlatWriter(bagFile(MANIF_FILE + sfx), null, tagWriters, true, tagEncoding));
    }

    /**
     * Returns conformant Bag from builder.
     * 
     * @return bag constructed from BagBuider data
     * @throws IOException if unknown autogen type or file problems
     */
    public Bag build() throws IOException {
        if (! built) {
            // if auto-generating any metadata, do so
            for (MetadataName autogenName : autogenNames) {
                switch (autogenName) {
                    case BAGGING_DATE ->
                        metadata(BAGGING_DATE, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                    case BAG_SIZE ->
                        metadata(BAG_SIZE, scaledSize(payloadSize, 0));
                    case PAYLOAD_OXUM ->
                        metadata(PAYLOAD_OXUM, String.valueOf(payloadSize) + "." + String.valueOf(payloadCount));
                    case BAG_SOFTWARE_AGENT ->
                        metadata(BAG_SOFTWARE_AGENT, "Packr Lib v:" + LIB_VSN);
                    default -> throw new IOException("unhandled autogen name: " + autogenName);
                } 
            }
            // close all optional writers' tag files
            for (FlatWriter fw : writers.values()) {
                fw.close();
            }
            // close all optional output streams
            for (BagOutputStream bout : streams.values()) {
                bout.close();
            }
            // close the manifest files
            for (FlatWriter mw : manWriters.values()) {
                mw.close();
            }
            // write out bagit declaration file
            FlatWriter fwriter = new FlatWriter(bagFile(DECL_FILE), null, tagWriters, false, StandardCharsets.UTF_8);
            fwriter.writeLine("BagIt-Version: " + BAGIT_VSN);
            fwriter.writeLine("Tag-File-Character-Encoding: " + tagEncoding.name());
            fwriter.close();
            // close tag manifest files of previous tag files
            for (FlatWriter tw : tagWriters.values()) {
                tw.close();
            }
            built = true;
        }
        return new Bag(base, false, ephemeral);
    }

    /**
     * Assigns the set of automatically generated metadata identifed by their names,
     * replacing default set: Bagging-Date, Bag-Size, Payload-Oxnum, Bag-Software-Agent
     * To disable automatic generation, pass in an empty set.
     * Unknown or non-auto-assignable names will be ignored.
     *
     * @param names the set of metadata names
     * @return builder this BagBuilder
     */
    public BagBuilder autoGenerate(Set<MetadataName> names) {
        autogenNames = names;
        return this;
    }

    /**
     * Returns the current operating set of automatically generated metadata identifed by their names,
     * An empty set indicates no autogenerated metadata
     *
     * @return names the set of autogenerated metadata values
     */
    public Set<MetadataName> autoGenerated() {
        return autogenNames;
    }

    /**
     * Adds a file to the payload at the root of the data
     * directory tree - convenience method when no payload hierarchy needed.
     *
     * @param topFile the file to add to the payload
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing payload
     */
    public BagBuilder payload(Path topFile) throws IOException {
        return payload(topFile.getFileName().toString(), topFile);
    }

    /**
     * Adds a file to the payload at the specified relative
     * path from the root of the data directory tree.
     *
     * @param relPath the relative path of the file
     * @param file the file path to add to the payload
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing payload
     */
    public BagBuilder payload(String relPath, Path file) throws IOException {
        Path payloadFile = dataFile(relPath);
        commitPayload(relPath, Files.newInputStream(file));
        // now rewrite payload attrs to original values
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        Files.setAttribute(payloadFile, "creationTime", attrs.creationTime());
        Files.setLastModifiedTime(payloadFile, attrs.lastModifiedTime());
        return this;
    }

    /**
     * Adds the contents of the passed stream to the payload
     * at the specified relative path in the data directory tree.
     *
     * @param relPath the relative path of the file
     * @param is the input stream to read.
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing payload
     */
    public BagBuilder payload(String relPath, InputStream is) throws IOException {
        commitPayload(relPath, is);
        return this;
    }

    private void commitPayload(String relPath, InputStream is) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        payloadSize += digestCopy(is, payloadFile, DATA_PATH + relPath, manWriters);
        payloadCount++;
    }

    private long digestCopy(InputStream is, Path destFile, String relPath, Map<String, FlatWriter> fwriters) throws IOException {
         // wrap stream in digest streams
         var digests = new HashMap<String, MessageDigest>();
         var iter = csAlgs.iterator();
         long copied = 0L;
         try {
             InputStream dis = is;
             while (iter.hasNext()) {
                 var alg = iter.next();
                 var dg = MessageDigest.getInstance(csAlgoCode(alg));
                 digests.put(alg, dg);
                 dis = new DigestInputStream(dis, dg);
             }
             copied = Files.copy(dis, destFile);
             // record checksums
             for (String alg : csAlgs) {
                 fwriters.get(alg).writeLine(toHex(digests.get(alg).digest()) + " " + relPath);
             }
         } catch (NoSuchAlgorithmException nsaE) {
             // should never occur, algorithms checked in constructor
             throw new IOException("bad algorithm");
         }
         return copied;
    }

    /**
     * Obtains manifest of bag contents.
     *
     * @param csAlgorithm the checksum used by manifest
     * @return List manifest list
     */
    public List<String> getManifest(String csAlgorithm) {
        return List.copyOf(manWriters.get(csAlgorithm).getLines());
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file.
     *
     * @param relPath the bag-relative path of the resource
     * @param file the file to add to fetch list
     * @param uri the URI of the resource
     * @return Filler this Filler
     * @throws IOException if error reading/writing ref data
     */
    public BagBuilder payloadRef(String relPath, Path file, URI uri) throws IOException {
        return payloadRef(relPath, Files.newInputStream(file), uri);
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file.
     *
     * @param relPath the bag-relative path of the resource
     * @param in the input stream of resource to add to fetch list
     * @param uri the URI of the resource
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing ref data
     */
    public BagBuilder payloadRef(String relPath, InputStream in, URI uri) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        if (! uri.isAbsolute()) {
            throw new IOException("URI must be absolute");
        }
        FlatWriter refWriter = getWriter(REF_FILE);
        var destDir = Files.createTempDirectory("null");
        var destFile = destDir.resolve("foo");
        long size = digestCopy(in, destFile, DATA_PATH + relPath, manWriters);
        var sizeStr = (size > 0L) ? Long.toString(size) : "-";
        refWriter.writeLine(uri.toString() + " " + sizeStr + " " + DATA_PATH + relPath);
        Files.delete(destFile);
        Files.delete(destDir);
        return this;
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file. Caller assumes full responsibility for
     * ensuring correctness of checksums and size - library does not verify.
     *
     * @param relPath the bag-relative path of the resource
     * @param size the expected size of the resource in bytes, use -1L for unknown
     * @param uri the URI of the resource
     * @param checksums map of algorithms to checksums of the resource
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing ref data
     */
    public BagBuilder payloadRefUnsafe(String relPath, long size, URI uri, Map<String, String> checksums) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        if (! uri.isAbsolute()) {
            throw new IOException("URI must be absolute: '" + uri.toString() + "'");
        }
        if (! checksums.keySet().equals(csAlgs)) {
            throw new IOException("checksums do not match bags");
        }
        for (String alg : manWriters.keySet()) {
            manWriters.get(alg).writeLine(checksums.get(alg) + " " + relPath);
        }
        var sizeStr = (size > 0L) ? Long.toString(size) : "-";
        FlatWriter refWriter = getWriter(REF_FILE);
        refWriter.writeLine(uri.toString() + " " + sizeStr + " " + DATA_PATH + relPath);
        return this;
    }

    /**
     * Obtains an output stream to a payload file at a relative path.
     *
     * @param relPath the relative path to the payload file
     * @return stream an output stream to payload file
     * @throws IOException if error reading/writing payload
     */
    public OutputStream payloadStream(String relPath) throws IOException {
        if (Files.exists(dataFile(relPath))) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        return getStream(dataFile(relPath), relPath, true);
    }

    /**
     * Adds a tag (metadata) file at the specified relative
     * path from the root of the bag directory tree.
     *
     * @param relPath the relative path of the file
     * @param file the path of the tag file to add
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing tag
     */
    public BagBuilder tag(String relPath, Path file) throws IOException {
        return tag(relPath, Files.newInputStream(file));
    }

    /**
     * Adds the contents of the passed stream to a tag (metadata) file
     * at the specified relative path in the bag directory tree.
     *
     * @param relPath the relative path of the file
     * @param is the input stream to read.
     * @return builder this BagBuilder
     * @throws IOException if error reading/writing tag
     */
    public BagBuilder tag(String relPath, InputStream is) throws IOException {
        // make sure tag files not written to payload directory
        if (relPath.startsWith(DATA_PATH)) {
            throw new IOException("Tag files not allowed in paylod directory");
        }
        if (Files.exists(bagFile(relPath))) {
            throw new IllegalStateException("Tag file already exists at: " + relPath);
        }
        digestCopy(is, tagFile(relPath), relPath, tagWriters);
        return this;
    }

    /**
     * Obtains an output stream to the tag file at a relative path.
     *
     * @param relPath the relative path to the tag file
     * @return stream an output stream to the tag file
     * @throws IOException if error reading/writing tag
     */
    public OutputStream tagStream(String relPath) throws IOException {
        if (Files.exists(tagFile(relPath))) {
            throw new IllegalStateException("Tag file already exists at: " + relPath);
        }
        return getStream(tagFile(relPath), relPath, false);
    }

    /**
     * Adds a reserved metadata property to the standard file
     * (bag-info.txt)
     *
     * @param name the property name
     * @param value the property value
     * @return builder this BagBuilder
     * @throws IOException if error writing metadata
     */
    public BagBuilder metadata(MetadataName name, String value) throws IOException {
        return property(META_FILE, name.getName(), value);
    }

    /**
     * Adds a metadata property to the standard file
     * (bag-info.txt)
     *
     * @param name the property name
     * @param value the property value
     * @return builder this BagBuilder
     * @throws IOException if error writing metadata
     */
    public BagBuilder metadata(String name, String value) throws IOException {
        return property(META_FILE, name, value);
    }

    /**
     * Adds a property to the passed property file.
     * Typically used for metadata properties in tag files.
     *
     * @param relPath the bag-relative path to the property file
     * @param name the property name
     * @param value the property value
     * @return builder this BagBuilder
     * @throws IOException if error writing property
     */
    public BagBuilder property(String relPath, String name, String value) throws IOException {
        FlatWriter writer = getWriter(relPath);
        writer.writeProperty(name, value);
        return this;
    }

    private Path dataFile(String name) throws IOException {
        // all user-defined files live in payload area - ie. under 'data'
        Path dataFile = bagFile(DATA_DIR).resolve(name);
        // create needed dirs
        Path parentFile = dataFile.getParent();
        if (! Files.isDirectory(parentFile)) {
            Files.createDirectories(parentFile);
        }
        return dataFile;
    }

    private Path tagFile(String name) throws IOException {
        // all user-defined tag files live anywhere in the bag
        Path tagFile = bagFile(name);
        // create needed dirs
        Path parentFile = tagFile.getParent();
        if (! Files.isDirectory(parentFile)) {
            Files.createDirectories(parentFile);
        }
        return tagFile;
    }

    private Path bagFile(String name) {
        return base.resolve(name);
    }

    private synchronized FlatWriter getWriter(String name) throws IOException {
        FlatWriter writer = writers.get(name);
        if (writer == null) {
            writer = new FlatWriter(bagFile(name), null, tagWriters, false, tagEncoding);
            writers.put(name, writer);
        }
        return writer;
    }

    private BagOutputStream getStream(Path path, String name, boolean isPayload) throws IOException {
        BagOutputStream stream = streams.get(name);
        if (stream == null) {
            var relPath = isPayload ? DATA_PATH + name : name;
            var writers = isPayload ? manWriters : tagWriters;
            stream = new BagOutputStream(path, relPath, writers);
            streams.put(name, stream);
        }
        return stream;
    }

    class FlatWriter extends BagOutputStream {

        private final List<String> lines = new ArrayList<>();
        private final boolean record;
        private final Charset encoding;
        private final AtomicBoolean bomOut = new AtomicBoolean();

        private FlatWriter(Path file, String brPath, Map<String, FlatWriter> tailWriters, boolean record, Charset encoding) throws IOException {
            super(file, brPath, tailWriters);
            this.record = record;
            this.encoding = encoding;
        }

        public void writeProperty(String key, String value) throws IOException {
            String prop = key + ": " + value;
            int offset = 0;
            while (offset < prop.length()) {
                int end = Math.min(prop.length() - offset, 80);
                if (offset > 0) {
                    write(filterBytes(SPACER, encoding, bomOut));
                }
                writeLine(prop.substring(offset, offset + end));
                offset += end;
            }
        }

        public void writeLine(String line) throws IOException {
            if (record) {
                lines.add(line);
            }
            write(filterBytes(line + lineSeparator, encoding, bomOut));
        }

        public List<String> getLines() {
            return lines;
        }
    }

    // wraps output stream in digester, and records results with tail writer
    class BagOutputStream extends OutputStream {

        private final String relPath;
        private final Map<String, FlatWriter> tailWriters;
        private OutputStream out;
        private HashMap<String, MessageDigest> digests;
        private boolean closed = false;

        private BagOutputStream(Path file, String relPath, Map<String, FlatWriter> tailWriters) throws IOException {
            this.relPath = (relPath != null) ? relPath : file.getFileName().toString();
            this.tailWriters = tailWriters;
            out = Files.newOutputStream(file);
            if (tailWriters != null) {
                // wrap stream in digest streams
                digests = new HashMap<String, MessageDigest>();
                var iter = csAlgs.iterator();
                try {
                    while (iter.hasNext()) {
                        var alg = iter.next();
                        var dg = MessageDigest.getInstance(csAlgoCode(alg));
                        digests.put(alg, dg);
                        out = new DigestOutputStream(out, dg);
                    }
                } catch (NoSuchAlgorithmException nsae) {
                    // should never occur - algorithms checked in constructor
                    throw new IOException("no such algorithm");
                }
            }
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public synchronized void close() throws IOException {
            if (! closed) {
                out.flush();
                if (tailWriters != null) {
                    // record checksums
                    for (String alg : csAlgs) {
                        tailWriters.get(alg).writeLine(toHex(digests.get(alg).digest()) + " " + relPath);
                    }
                }
                out.close();
                closed = true;
            }
        }
    }
}
