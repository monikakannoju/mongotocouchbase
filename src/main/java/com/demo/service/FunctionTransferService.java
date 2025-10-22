package com.demo.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.demo.controller.FunctionTransferProgressController;
import com.demo.dto.FunctionTransferRequest;

import io.micrometer.core.instrument.util.StringEscapeUtils;

@Service
public class FunctionTransferService {

	private final MongoFunctionService mongoFunctionService;
	private final CouchbaseFunctionService couchbaseFunctionService;
	@Autowired
	private FunctionTransferProgressController progressController;
	private static final Pattern JS_FUNCTION_PATTERN = Pattern.compile("(?:function\\s+(\\w+)\\s*\\(([^)]*)\\)"
			+ "|(\\w+)\\s*=\\s*function\\s*\\(([^)]*)\\)" + "|function\\s*\\(([^)]*)\\)"
			+ "|(\\w+)\\s*=\\s*\\(([^)]*)\\)\\s*=&gt;" + "|\\(([^)]*)\\)\\s*=&gt;" + ")\\s*\\{([\\s\\S]*?)\\}",
			Pattern.DOTALL);

	private AtomicInteger totalFunctionsProcessed = new AtomicInteger(0);
	private AtomicInteger totalFunctionsSucceeded = new AtomicInteger(0);
	private AtomicInteger totalFunctionsFailed = new AtomicInteger(0);
	private AtomicInteger totalFunctionsAutoTransferred = new AtomicInteger(0);
	private AtomicInteger totalFunctionsManualRequired = new AtomicInteger(0);
	private Set<String> processedFunctionNames = ConcurrentHashMap.newKeySet();

	private static final Map<String, String> TYPE_MAPPINGS = new HashMap<>();
	private static final Map<String, String> OPERATOR_MAPPINGS = new HashMap<>();
	private static final Map<String, String> FEATURE_MAPPINGS = new HashMap<>();
	private static final Set<String> N1QL_RESERVED_WORDS = new HashSet<>();
	private static final Pattern TEMPLATE_LITERAL_PATTERN = Pattern.compile("`(?:\\\\`|[^`])*`", Pattern.DOTALL);
	private static final Pattern TEMPLATE_EXPRESSION_PATTERN = Pattern.compile("\\$\\{((?:[^{}]|\\{[^}]*\\})*?)\\}",
			Pattern.DOTALL);
	private static final Pattern GRIDFS_PATTERN = Pattern.compile(
			"(GridFSBucket|gridfs_bucket)\\s*=\\s*new\\s+GridFSBucket\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
	private static final Pattern CHANGE_STREAMS_PATTERN = Pattern.compile("\\.watch\\s*\\(\\s*\\[?([^]]*)\\]?\\s*\\)",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern GEOSPATIAL_PATTERN = Pattern
			.compile("\\$(geoNear|geoWithin|geoIntersects|near|nearSphere|geometry)\\b", Pattern.CASE_INSENSITIVE);
	private static final Pattern ATOMIC_OPERATORS_PATTERN = Pattern
			.compile("\\$(inc|set|unset|push|pull|pop|rename|bit)\\b", Pattern.CASE_INSENSITIVE);
	private static final Pattern MANUAL_CONVERSION_MARKER = Pattern
			.compile("//\\s*(Converted|Implement|You need to implement)");

	private static final Object consoleLock = new Object();

	static {
		initializeTypeMappings();
		initializeOperatorMappings();
		initializeFeatureMappings();
		initializeReservedWords();
	}

	public FunctionTransferService(MongoFunctionService mongoFunctionService,
			CouchbaseFunctionService couchbaseFunctionService) {
		this.mongoFunctionService = mongoFunctionService;
		this.couchbaseFunctionService = couchbaseFunctionService;
	}

	private static void initializeTypeMappings() {
		TYPE_MAPPINGS.put("String", "String");
		TYPE_MAPPINGS.put("Boolean", "Boolean");
		TYPE_MAPPINGS.put("Number", "Number");
		TYPE_MAPPINGS.put("Integer", "Number");
		TYPE_MAPPINGS.put("Double", "Number");
		TYPE_MAPPINGS.put("Long", "Number");
		TYPE_MAPPINGS.put("ObjectId", "String");
		TYPE_MAPPINGS.put("Timestamp", "Date");
		TYPE_MAPPINGS.put("MinKey", "Number.MIN_VALUE");
		TYPE_MAPPINGS.put("MaxKey", "Number.MAX_VALUE");
		TYPE_MAPPINGS.put("DBRef", "Object");
		TYPE_MAPPINGS.put("BinData", "String");
		TYPE_MAPPINGS.put("UUID", "String");
		TYPE_MAPPINGS.put("MD5", "String");
		TYPE_MAPPINGS.put("Decimal128", "Number");
		TYPE_MAPPINGS.put("Symbol", "String");
		TYPE_MAPPINGS.put("Code", "Function");
		TYPE_MAPPINGS.put("CodeWScope", "Function");
		TYPE_MAPPINGS.put("RegularExpression", "String");
		TYPE_MAPPINGS.put("JavaScript", "Function");
		TYPE_MAPPINGS.put("JavaScriptWithScope", "Function");
		TYPE_MAPPINGS.put("BSONTimestamp", "Date");
		TYPE_MAPPINGS.put("BSONRegExp", "String");
		TYPE_MAPPINGS.put("Point", "Object");
		TYPE_MAPPINGS.put("LineString", "Object");
		TYPE_MAPPINGS.put("Polygon", "Object");
		TYPE_MAPPINGS.put("MultiPoint", "Object");
		TYPE_MAPPINGS.put("MultiLineString", "Object");
		TYPE_MAPPINGS.put("MultiPolygon", "Object");
		TYPE_MAPPINGS.put("GeometryCollection", "Object");
	}

	private static void initializeOperatorMappings() {
		OPERATOR_MAPPINGS.put("\\$abs", "ABS");
		OPERATOR_MAPPINGS.put("\\$add", "+");
		OPERATOR_MAPPINGS.put("\\$ceil", "CEIL");
		OPERATOR_MAPPINGS.put("\\$floor", "FLOOR");
		OPERATOR_MAPPINGS.put("\\$round", "ROUND");
		OPERATOR_MAPPINGS.put("\\$sqrt", "SQRT");
		OPERATOR_MAPPINGS.put("\\$pow", "POWER");
		OPERATOR_MAPPINGS.put("\\$mod", "%");
		OPERATOR_MAPPINGS.put("\\$exp", "EXP");
		OPERATOR_MAPPINGS.put("\\$ln", "LN");
		OPERATOR_MAPPINGS.put("\\$log", "LOG");
		OPERATOR_MAPPINGS.put("\\$log10", "LOG10");
		OPERATOR_MAPPINGS.put("\\$sin", "SIN");
		OPERATOR_MAPPINGS.put("\\$cos", "COS");
		OPERATOR_MAPPINGS.put("\\$tan", "TAN");
		OPERATOR_MAPPINGS.put("\\$asin", "ASIN");
		OPERATOR_MAPPINGS.put("\\$acos", "ACOS");
		OPERATOR_MAPPINGS.put("\\$atan", "ATAN");
		OPERATOR_MAPPINGS.put("\\$atan2", "ATAN2");
		OPERATOR_MAPPINGS.put("\\$toDegrees", "DEGREES");
		OPERATOR_MAPPINGS.put("\\$concat", "CONCAT");
		OPERATOR_MAPPINGS.put("\\$strLenCP", "LENGTH");
		OPERATOR_MAPPINGS.put("\\$toLower", "LOWER");
		OPERATOR_MAPPINGS.put("\\$toUpper", "UPPER");
		OPERATOR_MAPPINGS.put("\\$substr", "SUBSTR");
		OPERATOR_MAPPINGS.put("\\$substrBytes", "SUBSTR");
		OPERATOR_MAPPINGS.put("\\$substrCP", "SUBSTR");
		OPERATOR_MAPPINGS.put("\\$replaceOne", "REGEXP_REPLACE");
		OPERATOR_MAPPINGS.put("\\$replaceAll", "REGEXP_REPLACE");
		OPERATOR_MAPPINGS.put("\\$split", "SPLIT");
		OPERATOR_MAPPINGS.put("\\$regexMatch", "REGEXP_LIKE");
		OPERATOR_MAPPINGS.put("\\$strcasecmp", "LOWER");
		OPERATOR_MAPPINGS.put("\\$trim", "TRIM");
		OPERATOR_MAPPINGS.put("\\$ltrim", "LTRIM");
		OPERATOR_MAPPINGS.put("\\$rtrim", "RTRIM");
		OPERATOR_MAPPINGS.put("\\$indexOfBytes", "POSITION");
		OPERATOR_MAPPINGS.put("\\$indexOfCP", "POSITION");
		OPERATOR_MAPPINGS.put("\\$strLenBytes", "LENGTH");
		OPERATOR_MAPPINGS.put("\\$dateAdd", "DATE_ADD_MILLIS");
		OPERATOR_MAPPINGS.put("\\$dateDiff", "DATE_DIFF_MILLIS");
		OPERATOR_MAPPINGS.put("\\$dateTrunc", "DATE_TRUNC");
		OPERATOR_MAPPINGS.put("\\$dateFromString", "STR_TO_UTCSTR");
		OPERATOR_MAPPINGS.put("\\$dateToString", "DATE_FORMAT_STR");
		OPERATOR_MAPPINGS.put("\\$dateFromParts", "MAKE_DATE");
		OPERATOR_MAPPINGS.put("\\$dateToParts", "DATE_PART");
		OPERATOR_MAPPINGS.put("\\$dayOfMonth", "DAY");
		OPERATOR_MAPPINGS.put("\\$dayOfWeek", "DAYOFWEEK");
		OPERATOR_MAPPINGS.put("\\$dayOfYear", "DAYOFYEAR");
		OPERATOR_MAPPINGS.put("\\$year", "YEAR");
		OPERATOR_MAPPINGS.put("\\$month", "MONTH");
		OPERATOR_MAPPINGS.put("\\$week", "WEEK");
		OPERATOR_MAPPINGS.put("\\$hour", "HOUR");
		OPERATOR_MAPPINGS.put("\\$minute", "MINUTE");
		OPERATOR_MAPPINGS.put("\\$second", "SECOND");
		OPERATOR_MAPPINGS.put("\\$millisecond", "MILLISECOND");
		OPERATOR_MAPPINGS.put("\\$toDate", "STR_TO_UTCSTR");
		OPERATOR_MAPPINGS.put("\\$arrayElemAt", "ARRAY_ELEMENT_AT");
		OPERATOR_MAPPINGS.put("\\$concatArrays", "ARRAY_CONCAT");
		OPERATOR_MAPPINGS.put("\\$filter", "ARRAY_FILTER");
		OPERATOR_MAPPINGS.put("\\$map", "ARRAY_MAP");
		OPERATOR_MAPPINGS.put("\\$reduce", "ARRAY_REDUCE");
		OPERATOR_MAPPINGS.put("\\$size", "ARRAY_LENGTH");
		OPERATOR_MAPPINGS.put("\\$first", "ARRAY_FIRST");
		OPERATOR_MAPPINGS.put("\\$last", "ARRAY_LAST");
		OPERATOR_MAPPINGS.put("\\$sortArray", "ARRAY_SORT");
		OPERATOR_MAPPINGS.put("\\$setUnion", "ARRAY_SET_UNION");
		OPERATOR_MAPPINGS.put("\\$setIntersection", "ARRAY_SET_INTERSECT");
		OPERATOR_MAPPINGS.put("\\$setDifference", "ARRAY_SET_DIFFERENCE");
		OPERATOR_MAPPINGS.put("\\$setEquals", "ARRAY_SET_EQUALS");
		OPERATOR_MAPPINGS.put("\\$setIsSubset", "ARRAY_SET_IS_SUBSET");
		OPERATOR_MAPPINGS.put("\\$reverseArray", "ARRAY_REVERSE");
		OPERATOR_MAPPINGS.put("\\$zip", "ARRAY_ZIP");
		OPERATOR_MAPPINGS.put("\\$anyElementTrue", "ARRAY_ANY");
		OPERATOR_MAPPINGS.put("\\$allElementsTrue", "ARRAY_ALL");
		OPERATOR_MAPPINGS.put("\\$cond", "IF");
		OPERATOR_MAPPINGS.put("\\$ifNull", "IFMISSINGORNULL");
		OPERATOR_MAPPINGS.put("\\$switch", "CASE");
		OPERATOR_MAPPINGS.put("\\$coalesce", "COALESCE");
		OPERATOR_MAPPINGS.put("\\$cmp", "COMPARE");
		OPERATOR_MAPPINGS.put("\\$eq", "==");
		OPERATOR_MAPPINGS.put("\\$ne", "!=");
		OPERATOR_MAPPINGS.put("\\$gt", ">");
		OPERATOR_MAPPINGS.put("\\$gte", ">=");
		OPERATOR_MAPPINGS.put("\\$lt", "<");
		OPERATOR_MAPPINGS.put("\\$lte", "<=");
		OPERATOR_MAPPINGS.put("\\$in", "IN");
		OPERATOR_MAPPINGS.put("\\$nin", "NOT IN");
		OPERATOR_MAPPINGS.put("\\$and", "AND");
		OPERATOR_MAPPINGS.put("\\$or", "OR");
		OPERATOR_MAPPINGS.put("\\$not", "NOT");
		OPERATOR_MAPPINGS.put("\\$nor", "NOR");
		OPERATOR_MAPPINGS.put("\\$bitAnd", "&");
		OPERATOR_MAPPINGS.put("\\$bitOr", "|");
		OPERATOR_MAPPINGS.put("\\$bitXor", "^");
		OPERATOR_MAPPINGS.put("\\$bitNot", "~");
		OPERATOR_MAPPINGS.put("\\$bitsAllClear", "BIT_ALL_CLEAR");
		OPERATOR_MAPPINGS.put("\\$bitsAllSet", "BIT_ALL_SET");
		OPERATOR_MAPPINGS.put("\\$bitsAnyClear", "BIT_ANY_CLEAR");
		OPERATOR_MAPPINGS.put("\\$bitsAnySet", "BIT_ANY_SET");
		OPERATOR_MAPPINGS.put("\\$sum", "SUM");
		OPERATOR_MAPPINGS.put("\\$avg", "AVG");
		OPERATOR_MAPPINGS.put("\\$min", "MIN");
		OPERATOR_MAPPINGS.put("\\$max", "MAX");
		OPERATOR_MAPPINGS.put("\\$stdDevPop", "STDDEV_POP");
		OPERATOR_MAPPINGS.put("\\$stdDevSamp", "STDDEV_SAMP");
		OPERATOR_MAPPINGS.put("\\$push", "ARRAY_PUSH");
		OPERATOR_MAPPINGS.put("\\$addToSet", "ARRAY_DISTINCT");
		OPERATOR_MAPPINGS.put("\\$count", "COUNT");
		OPERATOR_MAPPINGS.put("\\$mergeObjects", "OBJECT_MERGE");
		OPERATOR_MAPPINGS.put("\\$maxN", "MAX_N");
		OPERATOR_MAPPINGS.put("\\$minN", "MIN_N");
		OPERATOR_MAPPINGS.put("\\$top", "TOP");
		OPERATOR_MAPPINGS.put("\\$topN", "TOP_N");
		OPERATOR_MAPPINGS.put("\\$bottom", "BOTTOM");
		OPERATOR_MAPPINGS.put("\\$bottomN", "BOTTOM_N");
		OPERATOR_MAPPINGS.put("\\$rank", "RANK");
		OPERATOR_MAPPINGS.put("\\$denseRank", "DENSE_RANK");
		OPERATOR_MAPPINGS.put("\\$documentNumber", "DOCUMENT_NUMBER");
		OPERATOR_MAPPINGS.put("\\$rowNumber", "ROW_NUMBER");
		OPERATOR_MAPPINGS.put("\\$lead", "LEAD");
		OPERATOR_MAPPINGS.put("\\$lag", "LAG");
		OPERATOR_MAPPINGS.put("\\$shift", "SHIFT");
		OPERATOR_MAPPINGS.put("\\$lookup", "LOOKUP");
		OPERATOR_MAPPINGS.put("\\$graphLookup", "GRAPH_LOOKUP");
		OPERATOR_MAPPINGS.put("\\$unionWith", "UNION_WITH");
		OPERATOR_MAPPINGS.put("\\$meta", "META");
		OPERATOR_MAPPINGS.put("\\$uuid", "UUID");
		OPERATOR_MAPPINGS.put("\\$function", "FUNCTION");
		OPERATOR_MAPPINGS.put("\\$let", "LET");
		OPERATOR_MAPPINGS.put("\\$literal", "LITERAL");
		OPERATOR_MAPPINGS.put("\\$rand", "RAND");
		OPERATOR_MAPPINGS.put("\\$range", "RANGE");
		OPERATOR_MAPPINGS.put("\\$indexOfArray", "ARRAY_POSITION");
		OPERATOR_MAPPINGS.put("\\$isArray", "IS_ARRAY");
		OPERATOR_MAPPINGS.put("\\$isNumber", "IS_NUMBER");
		OPERATOR_MAPPINGS.put("\\$isObject", "IS_OBJECT");
		OPERATOR_MAPPINGS.put("\\$type", "TYPE");
		OPERATOR_MAPPINGS.put("\\$convert", "CONVERT");
		OPERATOR_MAPPINGS.put("\\$toString", "TO_STRING");
		OPERATOR_MAPPINGS.put("\\$toInt", "TO_INT");
		OPERATOR_MAPPINGS.put("\\$toDouble", "TO_DOUBLE");
		OPERATOR_MAPPINGS.put("\\$toDecimal", "TO_DECIMAL");
		OPERATOR_MAPPINGS.put("\\$toLong", "TO_LONG");
		OPERATOR_MAPPINGS.put("\\$toBool", "TO_BOOL");
		OPERATOR_MAPPINGS.put("\\$set", "SET");
		OPERATOR_MAPPINGS.put("\\$unset", "UNSET");
		OPERATOR_MAPPINGS.put("\\$inc", "INCREMENT");
		OPERATOR_MAPPINGS.put("\\$mul", "MULTIPLY");
		OPERATOR_MAPPINGS.put("\\$rename", "RENAME");
		OPERATOR_MAPPINGS.put("\\$currentDate", "CURRENT_DATE");
		OPERATOR_MAPPINGS.put("\\$setOnInsert", "SET_ON_INSERT");
		OPERATOR_MAPPINGS.put("\\$pop", "POP");
		OPERATOR_MAPPINGS.put("\\$pullAll", "ARRAY_REMOVE_ALL");
		OPERATOR_MAPPINGS.put("\\$exists", "EXISTS");
		OPERATOR_MAPPINGS.put("\\$regex", "REGEXP_LIKE");
		OPERATOR_MAPPINGS.put("\\$text", "TEXT_SEARCH");
		OPERATOR_MAPPINGS.put("\\$where", "WHERE");
		OPERATOR_MAPPINGS.put("\\$geoNear", "GEO_NEAR");
		OPERATOR_MAPPINGS.put("\\$geoWithin", "GEO_WITHIN");
		OPERATOR_MAPPINGS.put("\\$geoIntersects", "GEO_INTERSECTS");
		OPERATOR_MAPPINGS.put("\\$near", "NEAR");
		OPERATOR_MAPPINGS.put("\\$nearSphere", "NEAR_SPHERE");
		OPERATOR_MAPPINGS.put("\\$geometry", "GEOMETRY");
	}

	private static void initializeFeatureMappings() {
		FEATURE_MAPPINGS.put("BSON Query Language", "N1QL (SQL-like)");
		FEATURE_MAPPINGS.put("\\$match", "WHERE");
		FEATURE_MAPPINGS.put("\\$project", "SELECT");
		FEATURE_MAPPINGS.put("\\$group", "GROUP BY");
		FEATURE_MAPPINGS.put("\\$sort", "ORDER BY");
		FEATURE_MAPPINGS.put("\\$skip", "OFFSET");
		FEATURE_MAPPINGS.put("\\$limit", "LIMIT");
		FEATURE_MAPPINGS.put("\\$lookup", "LEFT JOIN");
		FEATURE_MAPPINGS.put("\\$unwind", "ARRAY_FLATTEN");
		FEATURE_MAPPINGS.put("\\$out", "INSERT INTO");
		FEATURE_MAPPINGS.put("GridFSBucket", "Couchbase Blob Storage");
		FEATURE_MAPPINGS.put("\\.watch\\(", "Eventing Service or Kafka Connector");
		FEATURE_MAPPINGS.put("\\$geoNear", "GEO_DISTANCE");
		FEATURE_MAPPINGS.put("\\$geoWithin", "GEO_CONTAINS");
		FEATURE_MAPPINGS.put("\\$geoIntersects", "GEO_INTERSECTS");
		FEATURE_MAPPINGS.put("\\$near", "GEO_DISTANCE");
		FEATURE_MAPPINGS.put("\\$nearSphere", "GEO_DISTANCE");
		FEATURE_MAPPINGS.put("\\$inc", "mutateIn().increment");
		FEATURE_MAPPINGS.put("\\$set", "mutateIn().upsert");
		FEATURE_MAPPINGS.put("\\$unset", "mutateIn().remove");
		FEATURE_MAPPINGS.put("\\$push", "mutateIn().arrayAppend");
		FEATURE_MAPPINGS.put("\\$pull", "mutateIn().arrayRemove");
		FEATURE_MAPPINGS.put("\\$pop", "mutateIn().arrayRemove");
		FEATURE_MAPPINGS.put("\\$rename", "mutateIn().rename");
		FEATURE_MAPPINGS.put("\\$bit", "mutateIn().bitwise");
	}

	private static void initializeReservedWords() {
		N1QL_RESERVED_WORDS.addAll(Arrays.asList("ROLE", "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "ORDER", "BY",
				"GROUP", "INSERT", "DELETE", "UPDATE", "FUNCTION", "LANGUAGE", "AS", "ON", "IN", "LIKE", "BETWEEN",
				"CASE", "WHEN", "THEN", "ELSE", "END", "LIMIT", "OFFSET"));
	}

	public void transferAllFunctions(FunctionTransferRequest request) {
		resetStatistics();
		validateRequest(request);
		progressController.sendFunctionProgress("ALL", "STARTED", 0, 0, "Starting function transfer");
		try {
			transferShellFunctions(request);
			transferSystemJSFunctions(request);
			transferApplicationFunctions(request);
			transferAtlasFunctions(request);
			transferAggregationFunctions(request);
			transferAggregationExpressions(request);
			transferShellScripts(request);
			transferDriverFunctions(request);
			transferAtlasTriggers(request);
			transferRealmSyncFunctions(request);
			transferArrayProcessingFunctions(request);
			List<String> functionTypes = Arrays.asList("shell", "application", "atlas", "trigger", "realm", "array",
					"aggregation");

			int totalFunctions = 0;
			for (String type : functionTypes) {
				List<Document> functions = getFunctionsByType(type, request.getMongoDatabase());
				totalFunctions += functions.size();
			}

			progressController.sendFunctionProgress("ALL", "IN_PROGRESS", 0, totalFunctions,
					"Found " + totalFunctions + " functions to transfer");

			AtomicInteger counter = new AtomicInteger(0);

			for (String type : functionTypes) {
				transferFunctionsByType(request, type, counter, totalFunctions);
			}

			progressController.sendFunctionProgress("ALL", "COMPLETED", totalFunctions, totalFunctions,
					"Function transfer completed");
		} catch (Exception e) {
			progressController.sendFunctionError("ALL", "Function transfer failed: " + e.getMessage());
			throw e;
		} finally {
			printFinalStatistics();
		}
	}

	private void transferFunctionsByType(FunctionTransferRequest request, String type, AtomicInteger counter,
			int total) {
		List<Document> functions = getFunctionsByType(type, request.getMongoDatabase());

		for (Document function : functions) {
			String functionName = function.getString("_id");
			int currentCount = counter.incrementAndGet();

			progressController.sendFunctionProgress(functionName, "PROCESSING", currentCount, total,
					"Converting function");

			try {
				transferSingleFunctionOfType(function, type, request);
				progressController.sendFunctionProgress(functionName, "COMPLETED", currentCount, total,
						"Function transferred successfully");
			} catch (Exception e) {
				progressController.sendFunctionError(functionName, "Failed to transfer function: " + e.getMessage());
				if (!request.isContinueOnError()) {
					throw e;
				}
			}
		}
	}

	private List<Document> getFunctionsByType(String type, String database) {
		switch (type) {
		case "shell":
			return mongoFunctionService.getFunctionsByPattern(database, "shell");
		case "application":
			return mongoFunctionService.getFunctionsByPattern(database, "application");
		case "atlas":
			return mongoFunctionService.getFunctionsByPattern(database, "atlas");
		case "trigger":
			return mongoFunctionService.getFunctionsByPattern(database, "trigger");
		case "realm":
			return mongoFunctionService.getFunctionsByPattern(database, "realm");
		case "array":
			return mongoFunctionService.getArrayProcessingFunctions(database);
		case "aggregation":
			return mongoFunctionService.getAggregationFunctions(database);
		default:
			return Collections.emptyList();
		}
	}

	private void transferShellFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "shell");
		transferFunctions(functions, request, "Mongo Shell JavaScript");
	}

	private void transferSystemJSFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getSystemJSFunctions(request.getMongoDatabase());
		transferFunctions(functions, request, "Stored JavaScript");
	}

	private void transferApplicationFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(),
				"application");
		transferFunctions(functions, request, "Application Code");
	}

	private void transferAtlasFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "atlas");
		transferFunctions(functions, request, "Atlas App Services");
	}

	private void transferAggregationFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getAggregationFunctions(request.getMongoDatabase());
		transferFunctions(functions, request, "Aggregation Pipeline");
	}

	private void transferAggregationExpressions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getAggregationExpressions(request.getMongoDatabase());
		transferFunctions(functions, request, "Aggregation Expressions");
	}

	private void transferShellScripts(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "script");
		transferFunctions(functions, request, "Shell Scripts");
	}

	private void transferDriverFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "driver");
		transferFunctions(functions, request, "Driver Functions");
	}

	private void transferAtlasTriggers(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "trigger");
		transferFunctions(functions, request, "Atlas Triggers");
	}

	private void transferRealmSyncFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getFunctionsByPattern(request.getMongoDatabase(), "realm");
		transferFunctions(functions, request, "Realm Sync");
	}

	private void transferArrayProcessingFunctions(FunctionTransferRequest request) {
		List<Document> functions = mongoFunctionService.getArrayProcessingFunctions(request.getMongoDatabase());
		transferFunctions(functions, request, "Array Processing");
	}

	private void transferFunctions(List<Document> functions, FunctionTransferRequest request, String functionType) {
		if (functions == null || functions.isEmpty()) {
			synchronized (consoleLock) {
				System.out.println(
						"No " + functionType + " functions found in MongoDB database: " + request.getMongoDatabase());
			}
			return;
		}

		synchronized (consoleLock) {
			System.out.println("Starting transfer of " + functions.size() + " " + functionType + " functions");
		}

		int nThreads = Math.min(functions.size(), Runtime.getRuntime().availableProcessors());
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
		List<Future<Void>> futures = new ArrayList<>();

		for (Document function : functions) {
			futures.add(completionService.submit(() -> {
				try {
					transferSingleFunctionOfType(function, functionType, request);
				} catch (Exception e) {
					// Log the error but don't propagate it to stop other transfers
					totalFunctionsFailed.incrementAndGet();
					synchronized (consoleLock) {
						System.err.println("Failed to transfer function: " + function.getString("_id"));
						System.err.println("Error: " + e.getMessage());
					}
				}
				return null;
			}));
		}

		try {
			for (int i = 0; i < functions.size(); i++) {
				Future<Void> future = completionService.take();
				try {
					future.get(); // Just to propagate any exceptions that might have occurred
				} catch (ExecutionException e) {
					// Already handled in the callable, just continue with next function
				}
			}
		} catch (InterruptedException e) {
			cancelRemainingFutures(futures);
			Thread.currentThread().interrupt();
			throw new RuntimeException("Transfer interrupted", e);
		} finally {
			executor.shutdown();
		}
	}

	private void cancelRemainingFutures(List<Future<Void>> futures) {
		for (Future<Void> future : futures) {
			if (!future.isDone()) {
				future.cancel(true);
			}
		}
	}

	private void transferSingleFunctionOfType(Document function, String functionType, FunctionTransferRequest request) {
		String functionName = function.getString("_id");

		if (processedFunctionNames.contains(functionName)) {
			synchronized (consoleLock) {
				System.out.println("Skipping duplicate function: " + functionName);
			}
			return;
		}
		processedFunctionNames.add(functionName);

		String functionCode = getFunctionCode(function.get("value"));

		try {
			synchronized (consoleLock) {
				System.out.println(
						"\n==== PROCESSING " + functionType.toUpperCase() + " FUNCTION: " + functionName + " ====");
			}

			if ("Array Processing".equals(functionType)) {
				functionCode = optimizeArrayOperations(functionCode);
			}

			ConvertResult result = convertToN1QLFunction(functionName, functionCode, request.getCouchbaseScope(),
					couchbaseFunctionService.getBucketName());

			String n1qlFunction = result.getFunction();
			boolean requiresManual = result.isRequiresManual();

			synchronized (consoleLock) {
				System.out.println("CONVERTED FUNCTION:\n" + n1qlFunction);
				if (requiresManual) {
					System.out.println("MANUAL INTERVENTION REQUIRED FOR: " + functionName);
				}
			}

			couchbaseFunctionService.createFunction(n1qlFunction);
			totalFunctionsSucceeded.incrementAndGet();

			if (requiresManual) {
				totalFunctionsManualRequired.incrementAndGet();
			} else {
				totalFunctionsAutoTransferred.incrementAndGet();
			}

			synchronized (consoleLock) {
				System.out.println("SUCCESSFULLY TRANSFERRED: " + functionName);
			}
		} catch (Exception e) {
			totalFunctionsFailed.incrementAndGet();
			synchronized (consoleLock) {
				System.err.println("FAILED to transfer function: " + functionName);
				System.err.println("ERROR: " + e.getMessage());
			}
			if (!request.isContinueOnError()) {
				throw new RuntimeException(e);
			}
		} finally {
			totalFunctionsProcessed.incrementAndGet();
		}
	}

	private String optimizeArrayOperations(String functionCode) {
		String optimized = functionCode;

		for (Map.Entry<String, String> entry : OPERATOR_MAPPINGS.entrySet()) {
			optimized = optimized.replaceAll(entry.getKey(), entry.getValue());
		}

		optimized = optimized.replaceAll(
				"db\\.(\\w+)\\.aggregate\\(\\[\\s*\\{\\s*\\$unwind\\s*:\\s*\"\\$(\\w+)\"\\s*}\\s*\\]", "db.$1.*");

		optimized = optimized.replaceAll("\\{\\s*\\$push\\s*:\\s*\\{\\s*(\\w+)\\s*:\\s*\\$(\\w+)\\s*}\\s*}", "[$ $1]");

		return optimized;
	}

	public boolean transferSingleFunction(String database, String functionName, String scope) {
		try {
			Document function = findFunctionInAllTypes(database, functionName);
			if (function == null) {
				return false;
			}

			if (processedFunctionNames.contains(functionName)) {
				System.out.println("Skipping duplicate function: " + functionName);
				return false;
			}
			processedFunctionNames.add(functionName);

			String functionCode = getFunctionCode(function.get("value"));
			String functionType = function.containsKey("type") ? function.getString("type") : "";

			if ("array".equals(functionType)) {
				functionCode = optimizeArrayOperations(functionCode);
			}

			ConvertResult result = convertToN1QLFunction(functionName, functionCode, scope,
					couchbaseFunctionService.getBucketName());

			String n1qlFunction = result.getFunction();
			boolean requiresManual = result.isRequiresManual();

			couchbaseFunctionService.createFunction(n1qlFunction);
			totalFunctionsProcessed.incrementAndGet();
			totalFunctionsSucceeded.incrementAndGet();

			if (requiresManual) {
				totalFunctionsManualRequired.incrementAndGet();
			} else {
				totalFunctionsAutoTransferred.incrementAndGet();
			}

			return true;
		} catch (Exception e) {
			totalFunctionsProcessed.incrementAndGet();
			totalFunctionsFailed.incrementAndGet();
			System.err.println("Failed to transfer function: " + functionName);
			System.err.println("Error: " + e.getMessage());
			return false;
		}
	}

	private Document findFunctionInAllTypes(String database, String functionName) {
		Document function = mongoFunctionService.getFunctionByName(database, functionName);
		if (function != null)
			return function;

		String[] types = { "shell", "application", "atlas", "trigger", "realm", "array" };
		for (String type : types) {
			function = mongoFunctionService.getFunctionByPattern(database, functionName, type);
			if (function != null) {
				function.put("type", type);
				return function;
			}
		}
		return null;
	}

	private String convertMongoOperationsToN1QL(String body) {
		String converted = body;

		converted = converted.replaceAll("const\\s+session\\s*=\\s*db\\.getMongo\\(\\)\\.startSession\\(\\)\\s*;\\s*",
				"");
		converted = converted.replaceAll("session\\.startTransaction\\(\\)\\s*;\\s*", "");
		converted = converted.replaceAll("session\\.commitTransaction\\(\\)\\s*;\\s*", "");
		converted = converted.replaceAll("session\\.abortTransaction\\(\\)\\s*;\\s*", "throw e;");
		converted = converted.replaceAll("session\\.endSession\\(\\)\\s*;\\s*", "");
		converted = converted.replaceAll("\\,\\s*\\{\\s*session\\s*\\}", "");

		converted = converted.replaceAll("db\\.(\\w+)\\.findOne\\(\\{([^}]*)(?=\\})", "db.$1.findOne({$2");
		converted = converted.replaceAll("db\\.(\\w+)\\.findOne\\(([^)]*)$", "db.$1.findOne($1)");
		converted = converted.replaceAll("db\\.(\\w+)\\.find\\(\\{([^}]*)(?=\\})", "db.$1.find({$2");
		converted = converted.replaceAll("db\\.(\\w+)\\.find\\(([^)]*)$", "db.$1.find($1)");
		converted = converted.replaceAll("db\\.(\\w+)\\.aggregate\\(\\[([^]]*)(?=\\])", "db.$1.aggregate([$2");
		converted = converted.replaceAll("db\\.(\\w+)\\.aggregate\\(([^)]*)$", "db.$1.aggregate([$1])");

		for (Map.Entry<String, String> entry : OPERATOR_MAPPINGS.entrySet()) {
			converted = converted.replaceAll(entry.getKey(), entry.getValue());
		}

		converted = converted.replaceAll("context\\.services\\.get\\(", "getService(");
		converted = converted.replaceAll("context\\.environment\\.values", "environment");

		converted = convertGridFSToBlobStorage(converted);
		converted = convertChangeStreamsToEventing(converted);
		converted = convertGeospatialQueries(converted);
		converted = convertAtomicOperators(converted);

		return converted;
	}

	private String convertGridFSToBlobStorage(String code) {
		Matcher matcher = GRIDFS_PATTERN.matcher(code);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String replacement = "// Converted GridFSBucket to Couchbase Blob Storage\n"
					+ "// You need to implement using BinaryCollection or external storage";
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String convertChangeStreamsToEventing(String code) {
		Matcher matcher = CHANGE_STREAMS_PATTERN.matcher(code);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String args = matcher.group(1) != null ? matcher.group(1).trim() : "";
			String replacement = ".on('change', function(change) { \n"
					+ "  // Converted MongoDB change stream to Couchbase Eventing\n"
					+ "  // Implement using Eventing Service or Kafka Connector\n" + "  processChange(change); \n"
					+ "})";
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String convertGeospatialQueries(String code) {
		Matcher matcher = GEOSPATIAL_PATTERN.matcher(code);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String operator = matcher.group(1);
			String replacement = FEATURE_MAPPINGS.getOrDefault("\\$" + operator, "GEO_" + operator.toUpperCase());
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String convertAtomicOperators(String code) {
		Matcher matcher = ATOMIC_OPERATORS_PATTERN.matcher(code);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String operator = matcher.group(1);
			String replacement = FEATURE_MAPPINGS.getOrDefault("\\$" + operator, "mutateIn()." + operator);
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String convertSpecialFunctionTypes(String jsFunction) {
		if (jsFunction.contains("mapReduce(") || jsFunction.contains("mapFunction")
				|| jsFunction.contains("reduceFunction")) {
			return convertMapReduceFunction(jsFunction);
		}
		if (jsFunction.contains("$where") || jsFunction.contains("this.")) {
			return convertWhereFunction(jsFunction);
		}
		if (jsFunction.contains("db.eval(")) {
			return convertEvalFunction(jsFunction);
		}
		if (jsFunction.contains("$function") || jsFunction.contains("$let") || jsFunction.contains("$cond")) {
			return convertAggregationExpression(jsFunction);
		}
		if (jsFunction.contains("context.functions.execute")) {
			return convertAtlasFunction(jsFunction);
		}
		if (jsFunction.contains("changeEvent.") || jsFunction.contains("realmFunction")) {
			return convertRealmSyncFunction(jsFunction);
		}
		return jsFunction;
	}

	private String convertMapReduceFunction(String jsFunction) {
		jsFunction = jsFunction.replaceAll("function\\s+mapFunction\\s*\\(\\s*\\)\\s*\\{", "function map() {");
		jsFunction = jsFunction.replaceAll("function\\s+reduceFunction\\s*\\(\\s*\\)\\s*\\{", "function reduce() {");
		jsFunction = jsFunction.replaceAll("db\\.(\\w+)\\.mapReduce\\(", "db.$1.query(");
		return jsFunction;
	}

	private String convertWhereFunction(String jsFunction) {
		jsFunction = jsFunction.replaceAll("\\$where\\s*:\\s*function\\(\\)\\s*\\{", "WHERE ");
		jsFunction = jsFunction.replaceAll("this\\.(\\w+)", "$1");
		return jsFunction;
	}

	private String convertEvalFunction(String jsFunction) {
		jsFunction = jsFunction.replaceAll("db\\.eval\\(", "");
		jsFunction = jsFunction.replaceAll("\\)\\s*;?\\s*$", "");
		return jsFunction;
	}

	private String convertAggregationExpression(String jsFunction) {
		jsFunction = jsFunction.replaceAll(
				"\\$function\\s*:\\s*\\{\\s*body\\s*:\\s*(?:function)?\\(([^)]*)\\)\\s*\\{([\\s\\S]*?)\\}\\s*,\\s*args\\s*:\\s*\\[([^]]*)\\]\\s*,\\s*lang\\s*:\\s*['\"]js['\"]\\s*\\}",
				"function($1) { $2 }");
		jsFunction = jsFunction.replaceAll(
				"\\$let\\s*:\\s*\\{\\s*vars\\s*:\\s*\\{([^}]*)\\}\\s*,\\s*in\\s*:\\s*([^}]*)\\s*\\}",
				"(function() { var $1; return $2; })()");
		jsFunction = jsFunction.replaceAll("\\$cond\\s*:\\s*\\[\\s*([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^]]+)\\s*\\]",
				"($1) ? $2 : $3");
		return jsFunction;
	}

	private String convertAtlasFunction(String jsFunction) {
		jsFunction = jsFunction.replaceAll("context\\.functions\\.execute\\(", "executeFunction(");
		jsFunction = jsFunction.replaceAll("context\\.user\\.custom_data", "user.customData");
		jsFunction = jsFunction.replaceAll("context\\.user\\.id", "user.id");
		return jsFunction;
	}

	private String convertRealmSyncFunction(String jsFunction) {
		jsFunction = jsFunction.replaceAll("changeEvent\\.operationType", "syncEvent.operation");
		jsFunction = jsFunction.replaceAll("changeEvent\\.fullDocument", "syncEvent.document");
		jsFunction = jsFunction.replaceAll("changeEvent\\.documentKey", "syncEvent.documentId");
		return jsFunction;
	}

	private ConvertResult convertToN1QLFunction(String functionName, String jsFunction, String scopeName,
			String bucketName) {
		jsFunction = cleanJavaScriptFunction(jsFunction);
		jsFunction = convertSpecialFunctionTypes(jsFunction);
		jsFunction = convertMongoExpression(jsFunction);

		Matcher matcher = JS_FUNCTION_PATTERN.matcher(jsFunction);
		if (!matcher.find()) {
			String n1qlFunction = createFunctionDefinition(bucketName, scopeName, functionName, "",
					"function " + functionName + "() { " + jsFunction + " }");
			boolean requiresManual = detectManualConversion(n1qlFunction);
			return new ConvertResult(n1qlFunction, requiresManual);
		}

		String name = extractFunctionName(matcher, functionName);
		String params = extractParameters(matcher);
		String body = extractAndConvertFunctionBody(matcher, name);

		String cleanedParams = cleanFunctionParams(params);
		body = insertDefaultParamAssignments(body, params);

		String n1qlFunction = createFunctionDefinition(bucketName, scopeName, name, cleanedParams, body);
		boolean requiresManual = detectManualConversion(n1qlFunction);

		return new ConvertResult(n1qlFunction, requiresManual);
	}

	private boolean detectManualConversion(String n1qlFunction) {
		return MANUAL_CONVERSION_MARKER.matcher(n1qlFunction).find();
	}

	private String cleanJavaScriptFunction(String jsFunction) {
		if (!jsFunction.trim().endsWith("}")) {
			jsFunction = jsFunction + "}";
		}
		return jsFunction.trim().replaceAll("/\\*.*?\\*/", "").replaceAll("//.*", "").replaceAll("\\s+", " ")
				.replaceAll("\\s*\\{\\s*", "{").replaceAll("\\s*\\}\\s*", "}").replaceAll("\\s*\\(\\s*", "(")
				.replaceAll("\\s*\\)\\s*", ")").replaceAll("\\s*\\[\\s*", "[").replaceAll("\\s*\\]\\s*", "]");
	}

	private String extractFunctionName(Matcher matcher, String fallbackName) {
		return Stream.of(matcher.group(1), matcher.group(3), fallbackName).filter(Objects::nonNull).findFirst()
				.orElse("anonymous").trim();
	}

	private String extractParameters(Matcher matcher) {
		return Stream.of(matcher.group(2), matcher.group(4), matcher.group(5), matcher.group(7), matcher.group(8))
				.filter(Objects::nonNull).findFirst().orElse("").trim();
	}

	private String extractAndConvertFunctionBody(Matcher matcher, String functionName) {
		String body = matcher.group(9).trim();
		body = convertMongoOperationsToN1QL(body);
		if (!body.startsWith("function")) {
			body = "function " + functionName + "(" + extractParameters(matcher) + ") { " + body + " }";
		}
		return body;
	}

	private String quoteN1qlParamName(String param) {
		if (param == null || param.isEmpty())
			return param;
		String cleaned = param.trim();
		if (N1QL_RESERVED_WORDS.contains(cleaned.toUpperCase()) || !cleaned.matches("[a-zA-Z0-9_]+")) {
			return "`" + cleaned + "`";
		}
		return cleaned;
	}

	private String createFunctionDefinition(String bucketName, String scopeName, String functionName, String params,
			String body) {
		body = ensureCompleteFunction(body);
		String escapedBody = StringEscapeUtils.escapeJson(body);

		String[] paramTokens = params.isEmpty() ? new String[0] : params.split(",");
		List<String> quotedParams = new ArrayList<>();
		for (String p : paramTokens) {
			quotedParams.add(quoteN1qlParamName(p));
		}
		String paramsQuoted = String.join(", ", quotedParams);

		return String.format("CREATE OR REPLACE FUNCTION `%s`.`%s`.`%s`(%s) LANGUAGE JAVASCRIPT AS \"%s\"", bucketName,
				scopeName, functionName, paramsQuoted, escapedBody);
	}

	private String ensureCompleteFunction(String functionBody) {
		functionBody = fixTemplateLiterals(functionBody);
		functionBody = balanceAllBrackets(functionBody);
		functionBody = handleTransactionFunctions(functionBody);
		functionBody = fixAggregationPipelineFunctions(functionBody);
		return functionBody.trim();
	}

	private String balanceAllBrackets(String code) {
		code = balanceBraces(code);
		code = balanceParentheses(code);
		code = balanceSquareBrackets(code);
		return code;
	}

	private String balanceBraces(String code) {
		int level = 0;
		for (int i = 0; i < code.length(); i++) {
			char c = code.charAt(i);
			if (c == '{') {
				level++;
			} else if (c == '}') {
				if (level > 0) {
					level--;
				}
			}
		}
		while (level > 0) {
			code += "}";
			level--;
		}
		return code;
	}

	private String balanceParentheses(String code) {
		int level = 0;
		for (int i = 0; i < code.length(); i++) {
			char c = code.charAt(i);
			if (c == '(') {
				level++;
			} else if (c == ')') {
				if (level > 0) {
					level--;
				}
			}
		}
		while (level > 0) {
			code += ")";
			level--;
		}
		return code;
	}

	private String balanceSquareBrackets(String code) {
		int level = 0;
		for (int i = 0; i < code.length(); i++) {
			char c = code.charAt(i);
			if (c == '[') {
				level++;
			} else if (c == ']') {
				if (level > 0) {
					level--;
				}
			}
		}
		while (level > 0) {
			code += "]";
			level--;
		}
		return code;
	}

	private String fixTemplateLiterals(String code) {
		int backticks = code.length() - code.replace("`", "").length();
		if (backticks % 2 != 0) {
			code += "`";
		}
		Matcher matcher = TEMPLATE_LITERAL_PATTERN.matcher(code);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String literal = matcher.group();
			String convertedLiteral = convertTemplateExpressions(literal);
			matcher.appendReplacement(sb, Matcher.quoteReplacement(convertedLiteral));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String handleTransactionFunctions(String code) {
		code = code.replaceAll("const\\s+session\\s*=\\s*db\\.getMongo\\(\\)\\.startSession\\(\\)\\s*;\\s*", "");
		code = code.replaceAll("session\\.startTransaction\\(\\)\\s*;\\s*", "");
		code = code.replaceAll("session\\.commitTransaction\\(\\)\\s*;\\s*", "");
		code = code.replaceAll("session\\.abortTransaction\\(\\)\\s*;\\s*", "throw e;");
		code = code.replaceAll("session\\.endSession\\(\\)\\s*;\\s*", "");
		code = code.replaceAll("\\,\\s*\\{\\s*session\\s*\\}", "");
		return code;
	}

	private String fixAggregationPipelineFunctions(String code) {
		code = code.replaceAll(
				"\\$function\\s*:\\s*\\{\\s*body\\s*:\\s*(?:function)?\\(([^)]*)\\)\\s*\\{([\\s\\S]*?)\\}\\s*\\}",
				"$function: { body: function($1) { $2 }, args: ['$name', '$value'], lang: 'js' }");
		if (code.contains("aggregate(") && !code.contains("aggregate([") && !code.contains("aggregate([])")) {
			code = code.replace("aggregate(", "aggregate([");
			code = code.replaceFirst("(aggregate\\(\\[[^]]*?)(\\)[^)]*)$", "$1]$2");
		}
		return code;
	}

	private String preserveTemplateLiterals(String jsFunction) {
		Matcher matcher = TEMPLATE_LITERAL_PATTERN.matcher(jsFunction);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String literal = matcher.group();
			String convertedLiteral = convertTemplateExpressions(literal);
			matcher.appendReplacement(sb, Matcher.quoteReplacement(convertedLiteral));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String convertTemplateExpressions(String template) {
		Matcher matcher = TEMPLATE_EXPRESSION_PATTERN.matcher(template);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String expr = matcher.group(1);
			String convertedExpr = convertMongoExpression(expr);
			convertedExpr = preserveTemplateLiterals(convertedExpr);
			matcher.appendReplacement(sb, Matcher.quoteReplacement("${" + convertedExpr + "}"));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	private String cleanFunctionParams(String params) {
		if (params == null || params.isEmpty())
			return "";
		String[] tokens = params.split(",");
		List<String> cleaned = new ArrayList<>();
		for (String token : tokens) {
			String p = token.trim();
			int eqIdx = p.indexOf('=');
			if (eqIdx >= 0) {
				p = p.substring(0, eqIdx).trim();
			}
			if (!p.isEmpty())
				cleaned.add(p);
		}
		return String.join(", ", cleaned);
	}

	private String insertDefaultParamAssignments(String body, String originalParams) {
		if (originalParams == null || !originalParams.contains("="))
			return body;
		StringBuilder defaults = new StringBuilder();
		String[] tokens = originalParams.split(",");
		for (String token : tokens) {
			token = token.trim();
			if (token.contains("=")) {
				int eqIdx = token.indexOf('=');
				if (eqIdx > 0 && eqIdx < token.length() - 1) {
					String name = token.substring(0, eqIdx).trim();
					String val = token.substring(eqIdx + 1).trim();
					defaults.append("if (typeof ").append(name).append(" === 'undefined') ").append(name).append(" = ")
							.append(val).append(";\n");
				}
			}
		}
		if (defaults.length() > 0) {
			int braceIdx = body.indexOf('{');
			if (braceIdx >= 0) {
				return body.substring(0, braceIdx + 1) + "\n" + defaults + body.substring(braceIdx + 1);
			} else {
				return "{\n" + defaults + body + "\n}";
			}
		}
		return body;
	}

	private String convertMongoExpression(String expr) {
		String converted = expr;
		for (Map.Entry<String, String> entry : OPERATOR_MAPPINGS.entrySet()) {
			converted = converted.replaceAll(entry.getKey(), entry.getValue());
		}
		return converted;
	}

	private String getFunctionCode(Object functionValue) {
		if (functionValue == null) {
			throw new IllegalArgumentException("Function value cannot be null");
		}
		if (functionValue instanceof Code) {
			return ((Code) functionValue).getCode();
		} else if (functionValue instanceof CodeWScope) {
			return ((CodeWScope) functionValue).getCode();
		} else if (functionValue instanceof Document) {
			return convertDocumentToJS((Document) functionValue);
		} else if (functionValue instanceof List) {
			return convertListToJS((List<?>) functionValue);
		} else if (functionValue instanceof Binary) {
			return "\"" + Base64.getEncoder().encodeToString(((Binary) functionValue).getData()) + "\"";
		} else if (functionValue instanceof ObjectId) {
			return "\"" + ((ObjectId) functionValue).toHexString() + "\"";
		} else if (functionValue instanceof BSONTimestamp) {
			return "new Date(" + ((BSONTimestamp) functionValue).getTime() * 1000L + ")";
		} else if (functionValue instanceof Decimal128) {
			return ((Decimal128) functionValue).bigDecimalValue().toString();
		} else if (functionValue instanceof Symbol) {
			return "\"" + ((Symbol) functionValue).getSymbol() + "\"";
		} else if (functionValue instanceof UUID) {
			return "\"" + ((UUID) functionValue).toString() + "\"";
		} else if (functionValue instanceof Pattern) {
			return "\"" + ((Pattern) functionValue).pattern() + "\"";
		} else if (functionValue instanceof MinKey) {
			return "Number.MIN_VALUE";
		} else if (functionValue instanceof MaxKey) {
			return "Number.MAX_VALUE";
		} else {
			return functionValue.toString();
		}
	}

	private String convertDocumentToJS(Document doc) {
		StringBuilder sb = new StringBuilder("{");
		boolean first = true;

		for (Map.Entry<String, Object> entry : doc.entrySet()) {
			if (!first)
				sb.append(", ");
			sb.append(entry.getKey()).append(": ").append(getFunctionCode(entry.getValue()));
			first = false;
		}

		sb.append("}");
		return sb.toString();
	}

	private String convertListToJS(List<?> list) {
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;

		for (Object item : list) {
			if (!first)
				sb.append(", ");
			sb.append(getFunctionCode(item));
			first = false;
		}

		sb.append("]");
		return sb.toString();
	}

	private void validateRequest(FunctionTransferRequest request) {
		if (request.getMongoDatabase() == null || request.getMongoDatabase().isEmpty()) {
			throw new IllegalArgumentException("Mongo database name must be specified");
		}
		if (request.getCouchbaseScope() == null || request.getCouchbaseScope().isEmpty()) {
			throw new IllegalArgumentException("Couchbase scope must be specified");
		}
		if (couchbaseFunctionService.getBucketName() == null) {
			throw new IllegalStateException("Couchbase bucket name not configured");
		}
	}

	private void resetStatistics() {
		totalFunctionsProcessed.set(0);
		totalFunctionsSucceeded.set(0);
		totalFunctionsFailed.set(0);
		totalFunctionsAutoTransferred.set(0);
		totalFunctionsManualRequired.set(0);
		processedFunctionNames.clear();
	}

	private void printFinalStatistics() {
		synchronized (consoleLock) {
			int totalProcessed = totalFunctionsProcessed.get();
			int totalSucceeded = totalFunctionsSucceeded.get();
			int totalFailed = totalFunctionsFailed.get();
			int autoTransferred = totalFunctionsAutoTransferred.get();
			int manualRequired = totalFunctionsManualRequired.get();

			System.out.println("\n=== TRANSFER FINAL STATISTICS ===");
			System.out.println("Total functions processed: " + totalProcessed);
			System.out.println("Total functions succeeded: " + totalSucceeded);
			System.out.println("Total functions failed: " + totalFailed);
			System.out.println("Automatically transferred: " + autoTransferred);
			System.out.println("Manually transferred: " + manualRequired);

			if (totalSucceeded > 0) {
				double autoPercent = (autoTransferred * 100.0) / totalSucceeded;
				double manualPercent = (manualRequired * 100.0) / totalSucceeded;

				System.out.println("Percentage of succeeded functions automatically transferred: "
						+ String.format("%.2f", autoPercent) + "%");
				System.out.println("Percentage of succeeded functions manually transferred: "
						+ String.format("%.2f", manualPercent) + "%");
			}
		}
	}

	public int getTotalFunctionsProcessed() {
		return totalFunctionsProcessed.get();
	}

	public int getTotalFunctionsSucceeded() {
		return totalFunctionsSucceeded.get();
	}

	public int getTotalFunctionsFailed() {
		return totalFunctionsFailed.get();
	}

	public int getTotalAutoTransferred() {
		return totalFunctionsAutoTransferred.get();
	}

	public int getTotalManualRequired() {
		return totalFunctionsManualRequired.get();
	}

	private static class ConvertResult {
		private final String function;
		private final boolean requiresManual;

		public ConvertResult(String function, boolean requiresManual) {
			this.function = function;
			this.requiresManual = requiresManual;
		}

		public String getFunction() {
			return function;
		}

		public boolean isRequiresManual() {
			return requiresManual;
		}
	}
}