package com.continuuity.api.dataset.lib;

import com.continuuity.api.app.ApplicationConfigurer;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;

/**
 * Utility for describing {@link ObjectStore} and similar data sets within application configuration.
 */
public final class ObjectStores {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private ObjectStores() {}

  /**
   * Adds {@link ObjectStore} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param type type of objects to be stored in {@link ObjectStore}
   * @param props any additional data set properties
   * @throws UnsupportedTypeException
   */
  public static void createObjectStore(ApplicationConfigurer configurer,
                                       String datasetName, Type type, DatasetProperties props)
    throws UnsupportedTypeException {

    configurer.createDataSet(datasetName, ObjectStore.class, objectStoreProperties(type, props));
  }

  /**
   * Adds {@link MultiObjectStore} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param type type of objects to be stored in {@link ObjectStore}
   * @param props any additional data set properties
   * @throws UnsupportedTypeException
   */
  public static void createMultiObjectStore(ApplicationConfigurer configurer,
                                            String datasetName, Type type, DatasetProperties props)
    throws UnsupportedTypeException {

    configurer.createDataSet(datasetName, MultiObjectStore.class, objectStoreProperties(type, props));
  }

  /**
   * Adds {@link IndexedObjectStore} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param type type of objects to be stored in {@link IndexedObjectStore}
   * @param props any additional data set properties
   * @throws UnsupportedTypeException
   */
  public static void createIndexedObjectStore(ApplicationConfigurer configurer,
                                              String datasetName, Type type, DatasetProperties props)
    throws UnsupportedTypeException {

    configurer.createDataSet(datasetName, IndexedObjectStore.class, objectStoreProperties(type, props));
  }

  /**
   * Creates properties for {@link ObjectStore} or {@link MultiObjectStore} data set instance.
   * @param type type of objects to be stored in data set
   * @return {@link DatasetProperties} for the data set
   * @throws UnsupportedTypeException
   */
  public static DatasetProperties objectStoreProperties(Type type, DatasetProperties props)
    throws UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(type);
    TypeRepresentation typeRep = new TypeRepresentation(type);
    return DatasetProperties.builder()
      .add("schema", GSON.toJson(schema))
      .add("type", GSON.toJson(typeRep))
      .addAll(props.getProperties())
      .build();

  }
}