package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;
import java.util.List;

/**
 * This data set allows to store objects of a particular class into a table. The types that are supported are:
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * Interfaces and not-static inner classes are not supported.
 * @param <T> the type of objects in the store
 */
@Beta
public class ObjectStore<T> extends DataSet implements BatchReadable<byte[], T>, BatchWritable<byte[], T> {

  // the (write) schema of the objects in the store
  protected final Schema schema;
  // representation of the type of the objects in the store. needed for decoding (we need to tell the decoder what
  // type is should return - otherwise it would have to return an avro generic).
  protected final TypeRepresentation typeRep;
  // the underlying key/value table that we use to store the objects
  protected final KeyValueTable kvTable;

   // this is the dataset that executes the actual operations. using a delegate
   // allows us to inject a different implementation.
   private ObjectStore<T> delegate = null;

  /**
   * sets the ObjectStore to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param store the implementation to delegate to
   */
  public void setDelegate(ObjectStore<T> store) {
    this.delegate = store;
  }

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public ObjectStore(String name, Type type) throws UnsupportedTypeException {
    super(name);
    this.kvTable = new KeyValueTable("objects." + name);
    this.schema = new ReflectionSchemaGenerator().generate(type);
    this.typeRep = new TypeRepresentation(type);
  }

  /**
   * Constructor that takes in an existing key/value store. This can be called by subclasses to inject a different
   * key/value store implementation.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @param kvStore existing key/value store
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public ObjectStore(String name, Type type, KeyValueTable kvStore) throws UnsupportedTypeException {
    super(name);
    this.kvTable = kvStore;
    this.schema = new ReflectionSchemaGenerator().generate(type);
    this.typeRep = new TypeRepresentation(type);
  }

  /**
   * Constructor from a data set specification.
   * @param spec the specification
   */
  public ObjectStore(DataSetSpecification spec) {
    super(spec);
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    this.schema = gson.fromJson(spec.getProperty("schema"), Schema.class);
    this.typeRep = gson.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    this.kvTable = new KeyValueTable(spec.getSpecificationFor("objects." + this.getName()));
  }

  /**
   * Constructor from specification that also takes in an existing key/value store. This can be called by subclasses
   * to injecta different
   * key/value store implementation.
   * @param spec the data set specification
   * @param kvStore existing key/value store
   */
  protected ObjectStore(DataSetSpecification spec, KeyValueTable kvStore) {
    super(spec);
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    this.schema = gson.fromJson(spec.getProperty("schema"), Schema.class);
    this.typeRep = gson.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    this.kvTable = kvStore;
  }

  @Override
  public DataSetSpecification configure() {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    return new DataSetSpecification.Builder(this)
      .property("schema", gson.toJson(this.schema))
      .property("type", gson.toJson(this.typeRep))
      .dataset(this.kvTable.configure())
      .create();
  }

  /**
   * Constructor from another object store. Should only be called from the constructor
   * of implementing sub classes.
   * @param store the other object store
   */
  protected ObjectStore(ObjectStore<T> store) {
    super(store.getName());
    this.schema = store.schema;
    this.typeRep = store.typeRep;
    this.kvTable = store.kvTable;
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   * @throws OperationException in case of errors
   */
  public T read(byte[] key) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.read(key);
  }

  /**
   * Write an object with a given key.
   * @param key the key of the object
   * @param object the object to be stored
   * @throws OperationException in case of errors
   */
  public void write(byte[] key, T object) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(key, object);
  }

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.getSplits();
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.createSplitReader(split);
  }

  /**
   * Return a split reader that exposes the raw byte arrays from the underlying key value store.
   * @param split the split
   * @return a byte array split reader
   */
  @Beta
  public SplitReader<byte[], byte[]> createRawSplitReader(Split split) {
    return this.kvTable.createSplitReader(split);
  }
}
