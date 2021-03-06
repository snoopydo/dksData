﻿/*
   Copyright 2013 David Smith

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

using System;
using System.Data;
using System.Reflection;
using System.Reflection.Emit;

namespace dksData
{
	static class Extensions
	{

		public static void EmitFastInt(this ILGenerator il, int value)
		{
			switch (value)
			{
				case -1: il.Emit(OpCodes.Ldc_I4_M1); break;
				case 0: il.Emit(OpCodes.Ldc_I4_0); break;
				case 1: il.Emit(OpCodes.Ldc_I4_1); break;
				case 2: il.Emit(OpCodes.Ldc_I4_2); break;
				case 3: il.Emit(OpCodes.Ldc_I4_3); break;
				case 4: il.Emit(OpCodes.Ldc_I4_4); break;
				case 5: il.Emit(OpCodes.Ldc_I4_5); break;
				case 6: il.Emit(OpCodes.Ldc_I4_6); break;
				case 7: il.Emit(OpCodes.Ldc_I4_7); break;
				case 8: il.Emit(OpCodes.Ldc_I4_8); break;
				default:
					if (value >= -128 && value <= 127)
					{
						il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
					}
					else
					{
						il.Emit(OpCodes.Ldc_I4, value);
					}
					break;
			}
		}

		public static void EmitMemberAssignment(this ILGenerator il, Type item, Database.Setter setter)
		{
			if (setter.prop != null)
			{
				if (item.IsValueType)
				{
					il.Emit(OpCodes.Call, setter.prop.Setter);
				}
				else
				{
					il.Emit(OpCodes.Callvirt, setter.prop.Setter);
				}
			}
			else
			{
				il.Emit(OpCodes.Stfld, setter.field.Setter);
			}
		}

		public static bool IsNumeric(this Type type)
		{
			if (type == null)
			{
				return false;
			}

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
				case TypeCode.UInt64:
				case TypeCode.Single:
				case TypeCode.Double:
				case TypeCode.Decimal:
					return true;
				case TypeCode.Object:
					if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
					{
						return IsNumeric(Nullable.GetUnderlyingType(type));
					}
					return false;
			}
			return false;
		}
	}


	public static class Functions
	{

		public static MethodInfo IsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
		public static MethodInfo GetValue = typeof(IDataRecord).GetMethod("GetValue", new Type[] { typeof(int) });
		public static MethodInfo GetString = typeof(IDataRecord).GetMethod("GetString", new Type[] { typeof(int) });
		public static MethodInfo EnumParse = typeof(Enum).GetMethod("Parse", new Type[] { typeof(Type), typeof(string), typeof(bool) });
		public static MethodInfo GuidParse = typeof(Guid).GetMethod("Parse", new Type[] { typeof(string) });
		public static MethodInfo ConvertChangeType = typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(Object), typeof(Type) });
		public static MethodInfo GetTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");

	}

}