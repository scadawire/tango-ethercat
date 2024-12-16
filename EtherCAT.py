import time
from tango import AttrQuality, AttrWriteType, DispLevel, DevState, Attr, CmdArgType, UserDefaultAttrProp
from tango.server import Device, attribute, command, DeviceMeta
from tango.server import class_property, device_property
from tango.server import run
import os
import json
from threading import Thread
from threading import Lock
import pysoem
from json import JSONDecodeError
import struct

class EtherCAT(Device, metaclass=DeviceMeta):
    host = device_property(dtype=str, default_value="eth0")
    init_dynamic_attributes = device_property(dtype=str, default_value="")
    slave_index = device_property(dtype=int, default_value=0)
    slave = None
    dynamic_attribute_meta = {}

    @attribute
    def time(self):
        return time.time()

    @command(dtype_in=str)
    def add_dynamic_attribute(self, symbolName, register,
            variable_type_name="DevString", min_value="", max_value="",
            unit="", write_type_name="", label="", min_alarm="", max_alarm="",
            min_warning="", max_warning=""):
        if symbolName == "": return
        prop = UserDefaultAttrProp()
        variableType = self.stringValueToVarType(variable_type_name)
        writeType = self.stringValueToWriteType(write_type_name)
        if(min_value != "" and min_value != max_value): prop.set_min_value(min_value)
        if(max_value != "" and min_value != max_value): prop.set_max_value(max_value)
        if(unit != ""): prop.set_unit(unit)
        if(label != ""): prop.set_label(label)
        if(min_alarm != ""): prop.set_min_alarm(min_alarm)
        if(max_alarm != ""): prop.set_max_alarm(max_alarm)
        if(min_warning != ""): prop.set_min_warning(min_warning)
        if(max_warning != ""): prop.set_max_warning(max_warning)
        attr = Attr(symbolName, variableType, writeType)
        attr.set_default_properties(prop)
        self.add_attribute(attr, r_meth=self.read_dynamic_attr, w_meth=self.write_dynamic_attr)
        self.dynamic_attribute_meta[symbolName] = {"register": register, "variableType": variable_type}
        print("added dynamic attribute " + symbolName)

    def stringValueToVarType(self, variable_type_name) -> CmdArgType:
        if(variable_type_name == "DevBoolean"):
            return CmdArgType.DevBoolean
        if(variable_type_name == "DevLong"):
            return CmdArgType.DevLong
        if(variable_type_name == "DevDouble"):
            return CmdArgType.DevDouble
        if(variable_type_name == "DevFloat"):
            return CmdArgType.DevFloat
        if(variable_type_name == "DevString"):
            return CmdArgType.DevString
        if(variable_type_name == ""):
            return CmdArgType.DevString
        raise Exception("given variable_type '" + variable_type_name + "' unsupported, supported are: DevBoolean, DevLong, DevDouble, DevFloat, DevString")

    def stringValueToWriteType(self, write_type_name) -> AttrWriteType:
        if(write_type_name == "READ"):
            return AttrWriteType.READ
        if(write_type_name == "WRITE"):
            return AttrWriteType.WRITE
        if(write_type_name == "READ_WRITE"):
            return AttrWriteType.READ_WRITE
        if(write_type_name == "READ_WITH_WRITE"):
            return AttrWriteType.READ_WITH_WRITE
        if(write_type_name == ""):
            return AttrWriteType.READ_WRITE
        raise Exception("given write_type '" + write_type_name + "' unsupported, supported are: READ, WRITE, READ_WRITE, READ_WITH_WRITE")

    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        value = self.read_sdo(name)
        self.debug_stream("read value " + str(name) + ": " + str(value))
        attr.set_value(value)

    def write_dynamic_attr(self, attr):
        value = attr.get_write_value()
        name = attr.get_name()
        self.write_sdo(name, value)

    def read_sdo(self, name):
        index = self.dynamic_attribute_meta[name].register
        variableType = self.dynamic_attribute_meta[name].variableType
        length = self.bytes_per_variable_type(variableType)
        raw_data = self.slave.sdo_read(index, length)
        return struct.unpack(self.struct_key(variableType), raw_data)[0]

    def write_sdo(self, name, value):
        index = self.dynamic_attribute_meta[name].register
        variableType = self.dynamic_attribute_meta[name].variableType
        binary_value = struct.pack(self.struct_key(variableType), value)[0]
        self.slave.sdo_write(index, 0, binary_value)

    def struct_key(self, variableType):
        # see also https://de.mathworks.com/help/slrealtime/io_ref/ethercat-data-types.html
        # see also https://pysoem.readthedocs.io/en/latest/coe_objects.html
        # see also https://tango-controls.readthedocs.io/projects/rfc/en/latest/9/DataTypes.html

        # translation table
        # ESI - ctype - tango - struct
        # SINT - c_int8 - DevChar - b
        # INT - c_int16 - DevShort - h
        # DINT - c_int32 - DevLong - i
        # LINT - c_int64 - DevLong64 - q
        # USINT - c_uint8 - DevUChar - B
        # UINT - c_uint16 - DevUShort - H
        # UDINT - c_uint32 - DevULong - I
        # ULINT - c_uint64 - DevULong64 - Q
        # REAL - c_float - DevFloat - f
        # BOOL - c_bool - DevBoolean - c

        if(variableType == CmdArgType.DevChar) return 'b'
        if(variableType == CmdArgType.DevShort) return 'h'
        if(variableType == CmdArgType.DevLong) return 'i'
        if(variableType == CmdArgType.DevLong64) return 'q'
        if(variableType == CmdArgType.DevUChar) return 'B'
        if(variableType == CmdArgType.DevUShort) return 'H'
        if(variableType == CmdArgType.DevULong) return 'I'
        if(variableType == CmdArgType.DevULong64) return 'Q'
        if(variableType == CmdArgType.DevFloat) return 'f'
        if(variableType == CmdArgType.DevBoolean) return 'c'
        raise ValueError("Unsupported value type for binary translation")

    def translate_to_binary(self, value, variableType):
        # see also https://de.mathworks.com/help/slrealtime/io_ref/ethercat-data-types.html
        # see also https://pysoem.readthedocs.io/en/latest/coe_objects.html
        # see also https://tango-controls.readthedocs.io/projects/rfc/en/latest/9/DataTypes.html
        if(variableType == CmdArgType.DevLong)
            return value.to_bytes(4, byteorder='little')
        raise ValueError("Unsupported value type for binary translation")

    def bytes_per_variable_type(self, variableType):
        if(variableType == CmdArgType.DevShort):
            return 2
        if(variableType == CmdArgType.DevFloat):
            return 4
        elif(variableType == CmdArgType.DevDouble):
            return 8
        elif(variableType == CmdArgType.DevLong64):
            return 8
        elif(variableType == CmdArgType.DevLong): # 32bit int
            return 4
        elif(variableType == CmdArgType.DevBoolean): # attention! overrides full byte
            return 1
        return 0

    def init_device(self):
        self.set_state(DevState.INIT)
        self.get_device_properties(self.get_device_class())
        self.info_stream("Initializing EtherCAT master on interface " + self.host)
        try:
            self.master = pysoem.Master()
            self.master.open(self.host)
            self.master.config_init()
            if len(self.master.slaves) == 0:
                raise Exception("No slaves found")
            self.slave = self.master.slaves[slave_index]
            if self.init_dynamic_attributes != "":
                try:
                    attributes = json.loads(self.init_dynamic_attributes)
                    for attributeData in attributes:
                        self.add_dynamic_attribute(attributeData["name"], attributeData["register"],
                            attributeData.get("data_type", ""), attributeData.get("min_value", ""), attributeData.get("max_value", ""),
                            attributeData.get("unit", ""), attributeData.get("write_type", ""), attributeData.get("label", ""),
                            attributeData.get("min_alarm", ""), attributeData.get("max_alarm", ""),
                            attributeData.get("min_warning", ""), attributeData.get("max_warning", ""))
                except JSONDecodeError as e:
                    raise e
            self.set_state(DevState.ON)
        except Exception as e:
            self.error_stream("Failed to initialize EtherCAT master: " + str(e))
            self.set_state(DevState.FAULT)

if __name__ == "__main__":
    deviceServerName = os.getenv("DEVICE_SERVER_NAME")
    run({deviceServerName: EtherCAT})
