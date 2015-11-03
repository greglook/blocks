package blocks.data;


import clojure.lang.ArrayIter;
import clojure.lang.IHashEq;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import clojure.lang.IteratorSeq;
import clojure.lang.Murmur3;
import clojure.lang.Seqable;
import clojure.lang.Sequential;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.Iterator;


/**
 * Simple immutable byte sequence data structure.
 */
public class PersistentBytes implements IHashEq, Indexed, Iterable, Seqable, Sequential {

    private final byte[] _data;
    private int _hash = -1;
    private int _hasheq = -1;



    ///// Constructors /////

    /**
     * Constructs a new PersistentBytes object from the given binary data.
     *
     * @param data  array of bytes to wrap
     */
    private PersistentBytes(byte[] data) {
        if ( data == null ) {
            throw new IllegalArgumentException("Cannot construct persistent byte sequence on null data.");
        }
        _data = Arrays.copyOf(data, data.length);
    }


    /**
     * Constructs a new PersistentBytes object by wrapping the given binary
     * data. This is more efficient, but leaves the instance open to mutation
     * if the original array is modified.
     *
     * @return new persistent byte sequence, or null if data is null or empty
     */
    public static PersistentBytes wrap(byte[] data) {
        if ( data == null || data.length == 0 ) {
            return null;
        }
        return new PersistentBytes(data);
    }


    /**
     * Constructs a new PersistentBytes object with a copy o the given binary
     * data.
     *
     * @return new persistent byte sequence, or null if data is null or empty
     */
    public static PersistentBytes copyFrom(byte[] data) {
        if ( data == null || data.length == 0 ) {
            return null;
        }
        return new PersistentBytes(Arrays.copyOf(data, data.length));
    }



    ///// IO Methods /////

    /**
     * Opens an input stream to read the content.
     *
     * @return initialized input stream
     */
    public InputStream open() {
        return new ByteArrayInputStream(_data);
    }


    /**
     * Creates a buffer view of the content.
     *
     * @return read-only ByteBuffer
     */
    public ByteBuffer toBuffer() {
        return ByteBuffer.wrap(_data).asReadOnlyBuffer();
    }



    ///// Object /////

    @Override
    public int hashCode() {
        if ( _hash == -1 ) {
            _hash = Arrays.hashCode(_data);
        }
        return _hash;
    }


    @Override
    public boolean equals(Object obj) {
        if ( this == obj ) return true;
        if ( obj instanceof PersistentBytes ) {
            PersistentBytes other = (PersistentBytes)obj;
            return toBuffer().equals(other.toBuffer());
        }
        return false;
    }


    @Override
    public String toString() {
        return String.format("%s[size=%d]", this.getClass().getName(), count());
    }



    ///// IHashEq /////

    @Override
    public int hasheq() {
        if ( _hasheq == -1 ) {
            _hasheq = Murmur3.hashOrdered(this);
        }
        return _hasheq;
    }


    // TODO: equiv?



    ///// Iterable /////

    @Override
    public Iterator iterator() {
        return ArrayIter.createFromObject(_data);
    }



    ///// Seqable /////

    @Override
    public ISeq seq() {
        return IteratorSeq.create(iterator());
    }



    ///// Indexed /////

    @Override
    public int count() {
        return _data.length;
    }


    @Override
    public Object nth(int i) {
        return _data[i];
    }


    @Override
    public Object nth(int i, Object notFound) {
        if ( i >= 0 && i < count() ) {
            return nth(i);
        }
        return notFound;
    }

}
