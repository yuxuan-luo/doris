// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.external.paimon.util;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.paimon.predicate.PredicateBuilder;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class PaimonUtils {

    public static org.apache.paimon.predicate.Predicate paimonPredicateBuilder(PredicateBuilder builder,
                                                 Expr expr, Map<String, Integer> paimonFieldsId) {
        if (expr == null) {
            return null;
        }

        // CompoundPredicate
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    org.apache.paimon.predicate.Predicate left = paimonPredicateBuilder(builder,
                                                       compoundPredicate.getChild(0), paimonFieldsId);
                    org.apache.paimon.predicate.Predicate right = paimonPredicateBuilder(builder,
                                                       compoundPredicate.getChild(1), paimonFieldsId);
                    if (left != null && right != null) {
                        return PredicateBuilder.and(left, right);
                    }
                    return null;
                }
                case OR: {
                    org.apache.paimon.predicate.Predicate left = paimonPredicateBuilder(builder,
                                                    compoundPredicate.getChild(0), paimonFieldsId);
                    org.apache.paimon.predicate.Predicate right = paimonPredicateBuilder(builder,
                                                    compoundPredicate.getChild(1), paimonFieldsId);
                    if (left != null && right != null) {
                        return PredicateBuilder.or(left, right);
                    }
                    return null;
                }
                default:
                    return null;
            }
        }

        // BinaryPredicate
        if (expr instanceof BinaryPredicate) {
            TExprOpcode opCode = expr.getOpcode();
            switch (opCode) {
                case EQ:
                case NE:
                case GE:
                case GT:
                case LE:
                case LT:
                case EQ_FOR_NULL:
                    BinaryPredicate eq = (BinaryPredicate) expr;
                    SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                    LiteralExpr literalExpr = null;
                    if (slotRef == null && eq.getChild(0).isLiteral()) {
                        literalExpr = (LiteralExpr) eq.getChild(0);
                        slotRef = convertDorisExprToSlotRef(eq.getChild(1));
                    } else if (eq.getChild(1).isLiteral()) {
                        literalExpr = (LiteralExpr) eq.getChild(1);
                    }
                    if (slotRef == null || literalExpr == null) {
                        return null;
                    }
                    String colName = slotRef.getColumnName();
                    Object value = extractDorisLiteral(literalExpr);
                    if (value == null) {
                        if (opCode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                            return builder.isNull(paimonFieldsId.get(colName));
                        } else {
                            return null;
                        }
                    }
                    switch (opCode) {
                        case EQ:
                        case EQ_FOR_NULL:
                            return builder.equal(paimonFieldsId.get(colName), value);
                        case NE:
                            return builder.notEqual(paimonFieldsId.get(colName), value);
                        case GE:
                            return builder.greaterOrEqual(paimonFieldsId.get(colName), value);
                        case GT:
                            return builder.greaterThan(paimonFieldsId.get(colName), value);
                        case LE:
                            return builder.lessOrEqual(paimonFieldsId.get(colName), value);
                        case LT:
                            return builder.lessThan(paimonFieldsId.get(colName), value);
                        default:
                            return null;
                    }
                default:
                    return null;
            }
        }

        // InPredicate, only support a in (1,2,3)
        if (expr instanceof InPredicate) {
            InPredicate inExpr = (InPredicate) expr;
            if (inExpr.contains(Subquery.class)) {
                return null;
            }
            SlotRef slotRef = convertDorisExprToSlotRef(inExpr.getChild(0));
            if (slotRef == null) {
                return null;
            }
            List<Object> valueList = new ArrayList<>();
            for (int i = 1; i < inExpr.getChildren().size(); ++i) {
                if (!(inExpr.getChild(i) instanceof  LiteralExpr)) {
                    return null;
                }
                LiteralExpr literalExpr = (LiteralExpr) inExpr.getChild(i);
                Object value = extractDorisLiteral(literalExpr);
                valueList.add(value);
            }
            String colName = slotRef.getColumnName();
            if (inExpr.isNotIn()) {
                // not in
                return builder.notIn(paimonFieldsId.get(colName), valueList);
            } else {
                // in
                return builder.in(paimonFieldsId.get(colName), valueList);
            }
        }

        return null;
    }

    private static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    private static Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(ZoneId.systemDefault());
            StringBuilder sb = new StringBuilder();
            sb.append(dateLiteral.getYear())
                    .append(dateLiteral.getMonth())
                    .append(dateLiteral.getDay())
                    .append(dateLiteral.getHour())
                    .append(dateLiteral.getMinute())
                    .append(dateLiteral.getSecond());
            Date date;
            try {
                date = Date.from(
                    LocalDateTime.parse(sb.toString(), formatter).atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException e) {
                return null;
            }
            return date.getTime();
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            return decimalLiteral.getValue();
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return stringLiteral.getStringValue();
        }
        return null;
    }
}
