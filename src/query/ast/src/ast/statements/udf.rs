// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_meta_app::schema::CreateOption;

use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq)]
pub enum UDFDefinition {
    LambdaUDF {
        parameters: Vec<Identifier>,
        definition: Box<Expr>,
    },
    UDFServer {
        arg_types: Vec<TypeName>,
        return_type: TypeName,
        address: String,
        handler: String,
        language: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateUDFStmt {
    pub create_option: CreateOption,
    pub udf_name: Identifier,
    pub description: Option<String>,
    pub definition: UDFDefinition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterUDFStmt {
    pub udf_name: Identifier,
    pub description: Option<String>,
    pub definition: UDFDefinition,
}

impl Display for UDFDefinition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                write!(f, "AS (")?;
                write_comma_separated_list(f, parameters)?;
                write!(f, ") -> {definition}")?;
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                address,
                handler,
                language,
            } => {
                write!(f, "(")?;
                write_comma_separated_list(f, arg_types)?;
                write!(
                    f,
                    ") RETURNS {return_type} LANGUAGE {language} HANDLER = {handler} ADDRESS = {address}"
                )?;
            }
        }
        Ok(())
    }
}

impl Display for CreateUDFStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " FUNCTION")?;
        if let CreateOption::CreateIfNotExists(if_not_exists) = self.create_option {
            if if_not_exists {
                write!(f, " IF NOT EXISTS")?;
            }
        }
        write!(f, " {} {}", self.udf_name, self.definition)?;
        if let Some(description) = &self.description {
            write!(f, " DESC = '{description}'")?;
        }
        Ok(())
    }
}

impl Display for AlterUDFStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER FUNCTION")?;
        write!(f, " {} {}", self.udf_name, self.definition)?;
        if let Some(description) = &self.description {
            write!(f, " DESC = '{description}'")?;
        }
        Ok(())
    }
}
