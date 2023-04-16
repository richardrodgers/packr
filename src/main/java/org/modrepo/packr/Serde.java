/**
 * Copyright 2023, Richard Rodgers
 * SPDX-Licence-Identifier: Apache-2.0
 */

package org.modrepo.packr;

import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import static org.modrepo.packr.Bag.*;
/**
 * Serde is a class with static methods to serialize Bags to packages or IO streams
 * or deserialize IO streams or files into Bags.
 * 
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */
public class Serde {

    private static Set<String> knownFmts = Set.of(DFLT_FMT, "ZIP", TAR_FMT, TGZIP_FMT, "gz");
    private static byte[] zipSig = {0x50, 0x4b};
    private static byte[] tgzSig = {0x1f, (byte)0x8b};
    private static Set<byte[]> knownSigs = Set.of(zipSig, tgzSig);

    /**
     * Returns a Bag instance from passed directory containing bag contents.
     * Bag is not opaque.
     *
     * @param directory base directory from which to extract the bag
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error reading directory
     */
    public static Bag fromDirectory(Path directory) throws IOException {
        return fromDirectory(directory, false); 
    }
 
    /**
     * Returns a Bag instance from passed directory containing bag contents.
     *
     * @param directory base directory from which to extract the bag
     * @param opaque if true, make constructed bag opaque
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error reading directory
     */
    public static Bag fromDirectory(Path directory, boolean opaque) throws IOException {
        if (directory == null || ! Files.isDirectory(directory)) {
            throw new IOException("Missing or nonexistent bag directory");
        }
        return new Bag(directory, opaque, false);
    }

    /**
     * Returns a Bag instance from passed archive (or other package) file (possibly compressed)
     * Bag directory will be created as child of the passed archive parent directory,
     * and original artifact is untouched. Bag is unsealed.
     *
     * @param archive the archive file from which to extract the bag
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error unpacking archive file
     */
    public static Bag fromPackage(Path archive) throws IOException {
        return fromPackage(archive, false, Optional.empty());
    }

    /**
     * Returns a Bag instance from passed archive (or other package) file (possibly compressed)
     * If bagParent not specified, Bag directory will be created as a temporary directory,
     * and original artifact is untouched, else  as child of the passed archive parent directory,
     * and original artifact removed.
     *
     * @param archive the archive file from which to extract the bag
     * @param opaque if true, make constructed bag opaque
     * @param bagParent the parent directory into which the bag directory extracted
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error unpacking archive file
     */
    public static Bag fromPackage(Path archive, boolean opaque, Optional<Path> bagParent) throws IOException {
        if (archive == null || Files.notExists(archive)) {
            throw new IOException("Missing or nonexistent bag package");
        }
        // is it an archive file? If so,inflate into bag
        if (isReadableArchiveFile(archive)) {
            var fileName = archive.getFileName().toString();
            Path parentDir = bagParent.orElse(archive.getParent());
            Path base = parentDir.resolve(baseName(fileName));
            Files.createDirectories(base.resolve(DATA_DIR));
            inflate(parentDir, Files.newInputStream(archive), canonicalSuffix(fileName));
            // remove archive original
            //if (cleanup) {
            //    Files.delete(packageFile);
            //}
            return new Bag(base, opaque, false);
        } else {
            throw new IOException("Not a supported readable archive format");
        }
    }

    /**
     * Returns a Bag instance using passed I/O stream
     * and expected format of bag. Bag directory will be created
     * unsealed in a temporary directory.
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error processing stream
     */
    public static Bag fromStream(InputStream in, String format) throws IOException {
        return fromStream(in, format, false, Optional.empty());
    }

    /**
     * Returns a Bag instance using passed I/O stream
     * and expected format with bag. If bagParent not specified,
     * Bag directory will be created as a temporary directory.
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @param opaque if true, construct opaque bag
     * @param bagParent if present, parent directory of created bag, else temporary directory
     * @return bag directory contents interpreted as a Bag 
     * @throws IOException if error processing stream
     */
    public static Bag fromStream(InputStream in, String format, boolean opaque, Optional<Path> bagParent) throws IOException {
        Path theParent = bagParent.orElse(Files.createTempDirectory("bagparent"));
        return new Bag(inflate(theParent, in, format), opaque, false);
    }

    /**
     * Returns bag serialized as an archive file using default packaging (zip archive)
     * assigning zip timestamps
     *  
     * @param bag the Bag instance
     * @param pkgParent if present, the parent directory of the created package file 
     * @return archive the packaged Bag
     * @throws IOException if error writing package
     */

     public static Path toPackage(Bag bag, Optional<Path> pkgParent) throws IOException {
        return toPackage(bag, DFLT_FMT, false, pkgParent);
     }

    /**
     * Returns bag serialized as an archive file using passed packaging format.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *
     * @param bag the Bag instance
     * @param format the package format ('zip', or 'tgz')
     * @param noTime if 'true', suppress regular timestamp assignment in archive
     * @param pkgParent if present, the parent directory of the created package file
     * @return path the bag archive package path
     * @throws IOException if error reading bag
     */
    public static Path toPackage(Bag bag, String format, boolean noTime, Optional<Path> pkgParent) throws IOException {
        return deflate(bag, format, noTime);
    }

    /**
     * Returns bag serialized as an IO stream using default packaging (zip archive).
     * Bag is deleted when stream closed if temporary bag location used.
     *
     * @param bag the Bag instance
     * @return InputStream of the bag archive package
     * @throws IOException if error reading bag
     */
    public static InputStream toStream(Bag bag) throws IOException {
        return toStream(bag, DFLT_FMT, true);
    }

    /**
     * Returns bag serialized as an IO stream using passed packaging format.
     * Bag is deleted when stream closed if temporary bag location used.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *
     * @param bag the bag to serialize and stream
     * @param format the package format ('zip', or 'tgz')
     * @param noTime if 'true', suppress regular timestamp assignment in archive
     * @return InputStream of the bag archive package
     * @throws IOException if error reading bag
     */
    public static InputStream toStream(Bag bag, String format, boolean noTime) throws IOException {
        Path pkgFile = deflate(bag, format, noTime);
        if (bag.ephemeral) {
            return new CleanupInputStream(Files.newInputStream(pkgFile), pkgFile);
        } else {
            return Files.newInputStream(pkgFile);
        }
    }

    static class CleanupInputStream extends FilterInputStream {

        private final Path file;

        public CleanupInputStream(InputStream in, Path file) {
            super(in);
            this.file = file;
        }

        @Override
        public void close() throws IOException {
            super.close();
            Files.delete(file);
        }
    }

    private static String baseName(String fileName) {
        int idx = fileName.lastIndexOf(".");
        var stem = fileName.substring(0, idx);
        if (stem.endsWith(".tar")) {
            int idx2 = stem.lastIndexOf(".");
            return stem.substring(0, idx2);
        }
        return stem;
    }

    private static String canonicalSuffix(String fileName) {
        int sfxIdx = fileName.lastIndexOf(".");
        if (sfxIdx != -1) {
            var sfx = fileName.substring(sfxIdx + 1);
            if (sfx.equals("gz")) {
                return TGZIP_FMT; 
            }
            return sfx.toLowerCase();
        }
        return null;
    }

    private static boolean isReadableArchiveFile(Path file) {
        if (Files.isRegularFile(file)) {
            String baseName = file.getFileName().toString();
            int sfxIdx = baseName.lastIndexOf(".");
            if (sfxIdx > 0 && knownFmts.contains(baseName.substring(sfxIdx + 1))) {
                // peek inside
                try (InputStream in = Files.newInputStream(file)) {
                    var signature = in.readNBytes(2);
                    for (byte[] sig : knownSigs) {
                        if (Arrays.equals(sig, signature)) {
                            return true;
                        }
                    }
                } catch (IOException ioe) {}
            }
        }
        return false;
    }

    // inflate compressesd archive in base directory
    private static Path inflate(Path parent, InputStream in, String fmt) throws IOException {
        Path base = null;
        switch (fmt) {
            case "zip" :
                try (ZipInputStream zin = new ZipInputStream(in)) {
                    ZipEntry entry;
                    while((entry = zin.getNextEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(entry.getName().substring(0, entry.getName().indexOf("/")));
                        }
                        Path outFile = base.getParent().resolve(entry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(zin, outFile);
                        // Set file attributes to ZipEntry values
                        Files.setAttribute(outFile, "creationTime", entry.getCreationTime());
                        Files.setLastModifiedTime(outFile, entry.getLastModifiedTime());
                    }
                }
                break;
            case "tar" :
                try (TarArchiveInputStream tin = new TarArchiveInputStream(in)) {
                    TarArchiveEntry tentry;
                    while((tentry = tin.getNextTarEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(tentry.getName().substring(0, tentry.getName().indexOf("/")));
                        }
                        Path outFile = parent.resolve(tentry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(tin, outFile);
                        Files.setLastModifiedTime(outFile, FileTime.fromMillis(tentry.getLastModifiedDate().getTime()));
                    }
                }
                break;
            case "tgz" :
                try (TarArchiveInputStream tin = new TarArchiveInputStream(
                                                 new GzipCompressorInputStream(in))) {
                    TarArchiveEntry tentry;
                    while((tentry = tin.getNextTarEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(tentry.getName().substring(0, tentry.getName().indexOf("/")));
                        }
                        Path outFile = parent.resolve(tentry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(tin, outFile);
                        Files.setLastModifiedTime(outFile, FileTime.fromMillis(tentry.getLastModifiedDate().getTime()));
                    }
                }
                break;
            default:
                throw new IOException("Unsupported archive format: " + fmt);
        }
        return base;
    }

    private static Path deflate(Bag bag, String format, boolean noTime) throws IOException {
        // deflate this bag in situ (in current directory) using given packaging format
       // buildBag();
        Path pkgFile = bag.baseDir.getParent().resolve(bag.baseDir.getFileName().toString() + "." + format);
        deflate(bag, Files.newOutputStream(pkgFile), format, noTime);
        // remove base
        empty(bag.baseDir);
        return pkgFile;
    }

    private static void empty(Path root) throws IOException {
        deleteDir(root);
        Files.delete(root);
    }

    private static void deleteDir(Path dirFile) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    deleteDir(path);
                }
                Files.delete(path);
            }
        } catch (IOException ioE) {}
    }

    private static void deflate(Bag bag, OutputStream out, String format, boolean noTime) throws IOException {
        var base = bag.baseDir;
        switch(format) {
            case "zip":
                try (ZipOutputStream zout = new ZipOutputStream(
                                            new BufferedOutputStream(out))) {
                    fillZip(base, base.getFileName().toString(), zout, noTime);
                }
                break;
            case "tar":
                try (TarArchiveOutputStream tout = new TarArchiveOutputStream(
                                                   new BufferedOutputStream(out))) {
                    fillArchive(base, base.getFileName().toString(), tout, noTime);
                }
                break;
            case "tgz":
                try (TarArchiveOutputStream tout = new TarArchiveOutputStream(
                                                   new BufferedOutputStream(
                                                   new GzipCompressorOutputStream(out)))) {
                    fillArchive(base, base.getFileName().toString(), tout, noTime);
                }
                break;
            default:
                throw new IOException("Unsupported package format: " + format);
        }
    }

    private static void fillArchive(Path dirFile, String relBase, ArchiveOutputStream out, boolean noTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillArchive(file, relPath, out, noTime);
                } else {
                    TarArchiveEntry entry = new TarArchiveEntry(relPath);
                    entry.setSize(Files.size(file));
                    entry.setModTime(noTime ? 0L : Files.getLastModifiedTime(file).toMillis());
                    out.putArchiveEntry(entry);
                    Files.copy(file, out);
                    out.closeArchiveEntry();
                }
            }
        }
    }

    private static void fillZip(Path dirFile, String relBase, ZipOutputStream zout, boolean noTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillZip(file, relPath, zout, noTime);
                } else {
                    ZipEntry entry = new ZipEntry(relPath);
                    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    entry.setCreationTime(noTime ? FileTime.fromMillis(0L) : attrs.creationTime());
                    entry.setLastModifiedTime(noTime ? FileTime.fromMillis(0L) : attrs.lastModifiedTime());
                    zout.putNextEntry(entry);
                    Files.copy(file, zout);
                    zout.closeEntry();
                }
            }
        }
    }
}
